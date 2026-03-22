# haqlite Roadmap

## HaDatabase — First-Class Write Forwarding

Wrap Coordinator + rusqlite + internal HTTP forwarding into a single `HaDatabase` struct. Any Rust app embeds haqlite, calls `db.execute()` / `db.query_row()`, and gets transparent HA — writes forward to leader automatically, reads execute locally, role transitions handled internally.

- `SqlValue` enum + `ForwardedExecute` / `ExecuteResult` wire types (`forwarding.rs`)
- `HaDatabase` struct with `open()`, `execute()`, `query_row()`, `role()`, `close()` (`database.rs`)
- Internal axum forwarding server on dedicated port (leader receives forwarded writes)
- Connection lifecycle: leader rw, follower ro, automatic transitions on role events
- Refactor `ha_experiment.rs` to use HaDatabase (~500 → ~200 lines)
- 5 integration tests (`ha_database.rs`)

See CHANGELOG.md for completed work.
