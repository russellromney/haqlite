//! Node.js adapter. A worker owns each database so SQL never blocks Node's
//! event loop, and commands are queued in JavaScript invocation order.

mod options;
mod values;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc,
};

use haqlite::HaQLite as Database;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::{runtime::Handle, sync::oneshot};

use options::OpenOptions;

// Jobs contain only owned Rust values. In particular, JS buffers are copied
// before enqueueing, not read later on the database worker.
type Job = Box<dyn FnOnce(&mut Option<Database>, &Handle) + Send>;

#[napi(object)]
pub struct DatabaseStatus {
    pub role: Option<String>,
    pub is_caught_up: bool,
    pub replay_position: BigInt,
    pub db_path: String,
}

#[napi(js_name = "HaQLite")]
pub struct HaQLite {
    sender: mpsc::Sender<Job>,
    closed: Arc<AtomicBool>,
}

#[napi]
impl HaQLite {
    /// Open a file-backed database without replication or leader election.
    #[napi(factory)]
    pub async fn local(path: String, schema: Option<String>) -> Result<Self> {
        options::validate_path(&path)?;
        let db = tokio::task::spawn_blocking(move || {
            Database::local(&path, schema.as_deref().unwrap_or("")).map_err(database_error)
        })
        .await
        .map_err(database_error)??;
        Self::start(db)
    }

    /// Join a SingleWriter HA cluster using S3 WAL storage and S3 or HTTP leases.
    #[napi(factory)]
    pub async fn open(path: String, options: OpenOptions) -> Result<Self> {
        options::validate_path(&path)?;
        let db = options.open(&path).await?;
        Self::start(db)
    }

    /// Execute SQL, forwarding writes to the leader when this node is a follower.
    #[napi]
    #[allow(clippy::type_complexity)] // Inline unions generate the TypeScript SQL value types.
    pub fn execute(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<Either6<Null, bool, f64, String, BigInt, Buffer>>>,
    ) -> Result<AsyncBlock<f64>> {
        // Convert synchronously, then return validation errors as rejections too.
        let params = values::from_js(params.unwrap_or_default());
        self.dispatch(&env, move |db, runtime| {
            let params = params?;
            let db = db.as_ref().ok_or_else(closed_error)?;
            runtime
                .block_on(db.execute_async(&sql, &params))
                .map(|count| count as f64)
                .map_err(database_error)
        })
    }

    /// Read local rows. Follower reads may lag behind the leader.
    /// SQLite INTEGERs outside JavaScript's safe integer range return as bigint.
    #[napi]
    #[allow(clippy::type_complexity)] // Inline unions generate the TypeScript SQL value types.
    pub fn query(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<Either6<Null, bool, f64, String, BigInt, Buffer>>>,
    ) -> Result<AsyncBlock<Vec<Vec<Either5<Null, f64, String, BigInt, Buffer>>>>> {
        let params = values::from_js(params.unwrap_or_default());
        self.dispatch(&env, move |db, _| {
            let params = params?;
            let db = db.as_ref().ok_or_else(closed_error)?;
            db.query_values_local(&sql, &params)
                .map(|rows| {
                    rows.into_iter()
                        .map(|row| row.into_iter().map(values::to_js).collect())
                        .collect()
                })
                .map_err(database_error)
        })
    }

    /// Return a snapshot of this node's role and replication progress.
    #[napi]
    pub fn status(&self, env: Env) -> Result<AsyncBlock<DatabaseStatus>> {
        self.dispatch(&env, |db, _| {
            let db = db.as_ref().ok_or_else(closed_error)?;
            Ok(DatabaseStatus {
                role: db.role().map(|role| format!("{role:?}")),
                is_caught_up: db.is_caught_up(),
                replay_position: db.replay_position().into(),
                db_path: db.db_path().to_string_lossy().into_owned(),
            })
        })
    }

    /// Release leadership without closing. Returns false in local mode.
    #[napi]
    pub fn handoff(&self, env: Env) -> Result<AsyncBlock<bool>> {
        self.dispatch(&env, |db, runtime| {
            let db = db.as_ref().ok_or_else(closed_error)?;
            runtime.block_on(db.handoff()).map_err(database_error)
        })
    }

    /// Drain queued operations, flush replication, and release the lease.
    /// Repeated close calls are safe. All other calls reject after closing.
    #[napi]
    pub fn close(&self, env: Env) -> Result<AsyncBlock<()>> {
        let closed = self.closed.clone();
        let receiver = self.enqueue(|db, runtime| {
            if let Some(database) = db.as_mut() {
                runtime.block_on(database.close()).map_err(database_error)?;
                // Drop the SQLite connection before acknowledging close.
                db.take();
            }
            Ok(())
        });
        AsyncBlockBuilder::new(async move {
            match receiver.await {
                Ok(result) => result,
                Err(_) if closed.load(Ordering::Acquire) => Ok(()),
                Err(_) => Err(closed_error()),
            }
        })
        .build(&env)
    }
}

impl HaQLite {
    fn start(db: Database) -> Result<Self> {
        let (sender, receiver) = mpsc::channel::<Job>();
        let runtime = Handle::current();
        let closed = Arc::new(AtomicBool::new(false));
        let worker_closed = closed.clone();
        std::thread::Builder::new()
            .name("haqlite-node".into())
            .spawn(move || {
                let _runtime_guard = runtime.enter();
                let mut db = Some(db);
                for job in receiver {
                    job(&mut db, &runtime);
                    if db.is_none() {
                        worker_closed.store(true, Ordering::Release);
                        break;
                    }
                }
                // Best-effort cleanup on GC. Explicit close is required to wait
                // for replication and observe any shutdown error.
                if let Some(mut db) = db {
                    let _ = runtime.block_on(db.close());
                }
            })
            .map_err(database_error)?;
        Ok(Self { sender, closed })
    }

    fn enqueue<T, F>(&self, work: F) -> oneshot::Receiver<Result<T>>
    where
        T: Send + 'static,
        F: FnOnce(&mut Option<Database>, &Handle) -> Result<T> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let _ = self.sender.send(Box::new(move |db, runtime| {
            let _ = sender.send(work(db, runtime));
        }));
        receiver
    }

    fn dispatch<T, F>(&self, env: &Env, work: F) -> Result<AsyncBlock<T>>
    where
        T: Send + ToNapiValue + 'static,
        F: FnOnce(&mut Option<Database>, &Handle) -> Result<T> + Send + 'static,
    {
        let receiver = self.enqueue(work);
        AsyncBlockBuilder::new(async move { receiver.await.map_err(|_| closed_error())? })
            .build(env)
    }
}

fn database_error(error: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

fn closed_error() -> Error {
    Error::new(Status::Closing, "Engine closed")
}
