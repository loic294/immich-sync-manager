# Immich Sync Manager: rules for coding agents

Rust workspace (`crates/ism-core`: pure logic and DTOs; `crates/ism-server`: Axum server, SQLite,
Immich API/DB access, workers) plus the web UI in `web/` (its own rules: `web/AGENTS.md`).

## Commands
- `make check`: rustfmt, clippy (`-D warnings`), all Rust tests, then the web gate. Must be green.
- `make fmt`, `make dev-server` (data in `.data/`), `make dev-web`, `make image`.
- `make gen-api` after changing any DTO in `ism-core::dto`; commit the regenerated
  `web/src/api/generated/*.ts`.

## Invariants
- Target Immich v3.4.1 only. Any new direct database statement goes in `immich/pg.rs`, runs in a
  transaction, locks the row and checks it still matches the plan.
- Prefer the Immich API; direct writes only where the API cannot do it (paths, owners).
- Every library file change goes through a managed operation (`ops/`) inside the maintenance
  barrier (`maintenance.rs`). Fail closed: if something cannot be verified, stop and report.
- Deletes go to the ISM trash; purge is always an explicit action.
- Peer file-content transfers respect allowed hours; management events do not wait.

## Privacy
Fixtures, docs and tests use synthetic names, `*.example` hosts and documentation IP ranges only.
Never commit real paths, hostnames, accounts or credentials.
