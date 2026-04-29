# SYSTEM.md — haqlite

The current English model of haqlite. Owned by the human; agents verify against it before changing the code, and re-verify after. A line belongs in this file only if you'd care that an agent quietly violated it.

## What it is

Embeddable HA SQLite, in two crates. One node writes at a time; the others stay caught up and ready to take over if the writer dies. The base crate handles all of that — coordination, replication, transparent forwarding of writes from followers to the leader. The sibling crate (`haqlite-turbolite`) layers in `turbolite`, a separate crate that lets SQLite pages live in S3 instead of on local disk; with that, a brand-new node can come online and start serving without restoring a full database first.

The library family stands on its own against any S3-compatible store. It's part of the `hadb` family of crates, but it's agnostic about whether downstream consumers also pull in `hadb` or `turbodb` directly.

## Mode and Role

Two ideas, two axes.

**Mode** is the shape of the cluster — how the nodes coordinate writes. You pick this when you build the database. Today there's one Mode that ships: a single persistent writer, controlled by a token (called the lease) that one node holds at a time. Other nodes wait. There's a second Mode in the type system but stubbed: any node can write by grabbing a quick per-write token, with cloud storage acting as the source of truth. The second Mode only works in the turbolite sibling crate today, because plain SQLite would need to wait for every write to reach cloud before saying "done" — which the base crate doesn't do.

**Role** is what this particular node does inside that cluster. Four roles exist in the API: *Leader* (currently holds the lease), *Follower* (in the cluster, ready to be promoted if the leader dies), *Client* (a read-only node that never tries to claim a lease — useful as an embedded read replica), and *LatentWriter* (a quick-token writer for the second Mode). Leader and Follower work today; Client and LatentWriter are in the public types but bail at open with a "not implemented yet" error.

The whole grid is visible in the API even when implementations aren't there yet. That visibility is the contract; implementations land progressively.

## Lease + fence

Only one node should be writing at a time. The mechanism for that is a *lease*: a piece of data, kept in some shared store (S3, NATS, an HTTP service, etc.), that says "I'm the writer, and here's how long I'll be." The leader periodically renews it. If the leader stops renewing — because it died or got cut off from the network — a follower will eventually notice and claim the lease for itself.

Networks lie. A leader can be sure it's still the leader after the cluster has decided otherwise. The protection is a *fence token*: a counter that goes up every time the lease changes hands. The storage system that holds the actual data refuses any write tagged with a counter older than the latest one. So even if the old leader is confused and tries to write, the storage rejects it. This is the load-bearing safety property — nothing else compensates for getting it wrong. The mechanism details live in `hadb`; haqlite consumes them.

A practical caveat: Tigris S3 isn't a safe lease store. Its conditional writes aren't atomic when two clients race — both can succeed. Use NATS, Cinch HTTP, or AWS S3.

There's also a second fence inside SQLite itself. When a node loses its lease, the *authorizer* (a SQLite plugin that vets every statement) gets re-installed in a mode that blocks writes outright. Two layers of defense — the storage rejects fenced writes from outside, and the SQLite connection won't even attempt them.

## Replication

Replication is how data gets from the writer to other nodes — and to durable cloud storage, so a brand-new node can recover from scratch.

There are two ways data flows to the cloud, and they compose. The first is **log shipping**: SQLite has a write-ahead log of every change, and the leader can ship those log entries to S3 on a schedule (never, on an interval, or after every single commit). Followers pull the log down and replay it locally. The crate that handles this for haqlite is `walrust`. The second way is **page tiering**: instead of (or alongside) shipping the log, the data pages themselves get uploaded — either at checkpoints, continuously, or after every commit. With tiering, followers (and brand-new nodes) can pull pages directly from S3. The crate that handles this is `turbolite`.

Defaults: when the turbolite sibling crate is in use, the default is "continuous" — pages get uploaded at checkpoints, and the log gets shipped every second. cinch-cloud relies on this default to hit its cold-start performance.

Which replicator runs at the turbolite layer is decided like this: a caller-supplied replicator wins; "Cloud" durability uses a no-op replicator (pages are the replication, so there's no log to ship); everything else uses walrust.

Followers don't need a perfect log to catch up. They pull from cloud as new changes arrive and apply them by looking at what each page contains, not by following a strict chain of log entries. So when a node gets promoted, it doesn't have to re-upload everything — the next follower can still figure out the gap. That's what makes leader handoff cheap.

## Manifest

The *manifest* is a small file that says "here's what the database looks like right now" — a version number plus a payload describing the current state. The leader publishes a new manifest every time something meaningful changes; followers poll for new versions and apply them. When a manifest-changed event arrives, the polling loop fast-wakes instead of waiting for the next tick.

The envelope (version + payload + a few fields) is owned by `turbodb` and uses a stable wire format. The payload itself is opaque to haqlite — it's whatever the consumer (turbolite) needs.

## Close + flush ordering

Graceful shutdown has to push everything to cloud before letting another node take over. The order: turbolite flushes its pages, then the cluster departure flushes the walrust log and releases the lease — in that order. Why it matters: if pages aren't flushed first, the published manifest can point at pages the next leader can't find. If the log isn't flushed before lease release, the next leader is missing the most recent writes. If the lease is released before either of those, the cluster can hand control to a node that hasn't seen the latest data.

`handoff()` is the explicit version of this for SIGTERM handlers — same sequence, called intentionally. If a node gets dropped without `close()` being called, the lease isn't cleanly released; it just expires after its TTL, and the cluster waits.

## Hrana and forwarding

There are two HTTP surfaces clients can hit. The first is internal write forwarding: when a follower receives a write through the native client API, it transparently sends it to the leader and returns the result. The second is *hrana*, a libSQL-compatible HTTP API that lets standard libSQL clients connect to haqlite. Hrana followers serve reads but reject writes outright — they don't forward. If a client wants writes to be transparently routed to the leader, it has to use haqlite's native client, not hrana.

## Server

The base crate ships a `haqlite serve` binary for the deploy-as-a-service case. It hosts a single database, exposes hrana and the write-forwarding endpoint on the configured port, and reads its config from CLI flags and env vars at the binary boundary. The library itself never touches env vars; the binary does. One database per process.

## Native client

Embedded usage gets the API directly on the database handle. Remote consumers use the native `HaQLiteClient`, which handles leader discovery, reconnect, and write forwarding. Hrana can't forward writes; if you want forwarding from outside the cluster, the native client is the path.

## Local mode

The base crate has a non-HA single-node mode for development and embedded uses that don't need distribution. The API is the same; the cluster machinery (coordinator, lease store, replicator) is just absent. Useful for tests and for deploying haqlite as a plain library when you only have one node anyway.

## Configuration

The library never reads environment variables on its own. A small `env` module provides opt-in helpers that resolve env vars and return values you pass to the builder; the convenience is explicit at the call site.

## Invariants — must not quietly change

- Base haqlite never depends on turbolite. Tiering is the sibling crate's concern.
- All Mode and Role variants are visible in the public API; unimplemented combinations bail at open with a message naming what would need to ship.
- Storage adapters enforce fence tokens on writes. Stale leaders can't write past their fence.
- The manifest envelope is an opaque-payload msgpack wire format. Stable contract.
- Continuous is the default tiered durability.
- Close flushes turbolite pages before releasing the lease.
- Hrana followers reject writes; only the native client forwards.
- The library never reads environment variables on its own. Callers opt in via the `env` helpers.
- The `haqlite serve` binary hosts exactly one database per process.
- Close flushes walrust frames before releasing the lease; otherwise the changeset chain is incomplete for the next leader.
- Demotion re-installs the authorizer in fenced mode; writes are blocked at the SQLite layer in addition to the storage layer.
- Tigris S3 is incompatible with the S3 lease store.

## How this file changes

Phase entries propose changes via spec diffs in `.intent/phases/<name>/spec-diff.md` (`What changes` / `What does not change` / `How to verify`). Implementation lands per the IDD loop. This file updates only after the implementation proves it deserves the new baseline; the prior version is preserved alongside the phase artifacts.

If this file ever describes something the code doesn't do, fix one or the other.
