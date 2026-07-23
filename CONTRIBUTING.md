# Contributing to Fathom

Thanks for helping build fathom. This is the practical "get set up and land a change" guide. The
full working agreement — architecture, principles, and the detailed component map — is
[`AGENTS.md`](AGENTS.md); the curated docs map is [`docs/README.md`](docs/README.md).

## 1. Prerequisites & first checkout

- **Elixir ≥ 1.15** on a compatible OTP. The project builds and tests on **Elixir 1.19 / OTP 27**
  (the version the release image pins); there is no `.tool-versions` yet, so match that if you use a
  version manager.
- **A C toolchain** — the `exqlite` SQLite NIF compiles from source (`build-essential` on Linux,
  Xcode Command Line Tools on macOS).
- **PostgreSQL running locally** — fathom's control-plane / directory store (shard *data* is SQLite,
  not Postgres). By default fathom connects as your **OS user** with no password; set
  `PGUSER` / `PGPASSWORD` / `PGHOST` / `PGDATABASE` to override, or edit `config/dev.exs` /
  `config/test.exs`. You need a role that can create databases (`mix setup` creates `fathom_dev`;
  `mix test` creates `fathom_test`).
- **The `filo` sibling repo.** fathom depends on [Filo](https://github.com/cwisecarver/filo) — the
  Hrana/libSQL protocol server — as a **path dependency at `../filo`**, so check both out side by
  side. A clone of `fathom` alone fails `mix deps.get`:

  ```
  parent/
  ├── fathom/   # this repo
  └── filo/     # git clone https://github.com/cwisecarver/filo
  ```

## 2. Set up, run, and see a round-trip

```bash
mix setup                 # deps.get + create/migrate the dev DB + install/build assets
iex -S mix phx.server     # web/dashboard/API :4000 · Hrana/libSQL :8080 · health :8081
```

The three listeners: **:4000** is the Phoenix web/dashboard/`/api` endpoint, **:8080** is the
Hrana/libSQL data port (`Filo`), **:8081** is the LB health probe. (All three are off in `mix test`.)

Provision a tenant and point a client at it:

```bash
curl -su admin:admin -X POST http://localhost:4000/api/tenants \
  -H 'content-type: application/json' -d '{"shard_id":"acme"}'
# then connect any libSQL / django-libsql client at  ws://acme.localhost:8080
# (the Host subdomain selects the shard; ws://localhost:8080 → the `demo` default shard in dev)
```

See [`docs/quickstart-django.md`](docs/quickstart-django.md) for the Django end-to-end and
[`docs/configuration.md`](docs/configuration.md) for every config knob. Prefer a zero-toolchain
first look? The Docker eval stack in [`deploy/compose/README.md`](deploy/compose/README.md) brings up
fathom + Postgres + MinIO + nginx with one command.

## 3. The change loop

Fathom's cycle is **implement → compile → test → (bench if hot path) → `mix precommit` → commit →
push**. Test after every change; fix failures before proceeding.

- **The gate is `mix precommit`** — it runs `compile --warnings-as-errors`, `deps.unlock --unused`,
  `format`, and `test` (in `MIX_ENV=test`). Never commit if it fails. There is **no server-side CI**
  (Actions is disabled repo-wide by design), so this local gate is the only enforcement.
- **Hot-path changes go through the bench gate.** If you touch shard routing/open, the directory
  resolve, the migration copy, or the shard coordinator, land the change with
  `scripts/commit_with_bench.sh -m "<msg>"` — it benches the working tree and refuses the commit on a
  ≥20% regression in any metric vs the parent baseline. Pure docs/test/comment-only changes skip it
  (`git commit … [skip-bench]`). See [`docs/benchmark-plan.md`](docs/benchmark-plan.md) and the
  Benchmarking section of `AGENTS.md`.
- **Always `git push` right after a commit** — an unpushed commit is unbacked-up work.

### Testing discipline

Every feature and bug fix ships with tests in the same commit (a bug fix ships a **regression test
that fails pre-fix and pins the violated invariant**). Two stores, two test modes:

- **Postgres directory** (`Fathom.Repo`) → `Fathom.DataCase` with the Ecto SQL sandbox
  (async-safe, auto-rollback per test).
- **libSQL shards** (`Fathom.Shard` / `Fathom.Shards`) → no sandbox; a shard is a real SQLite file.
  Use a **unique `shard_id` per test**, drive it through `Fathom.Shards` / `Fathom.ShardExecutor`,
  and `File.rm` the file in `on_exit`. Never let two tests share a shard file.

Non-negotiable invariants to cover: **shard isolation** (a query for shard A must never resolve to
B), **migrations tested both ways** (forward copy+transform and revert pointer-flip), and
**cross-version tolerance** during a mixed `vN-1`/`vN` rollout. The full discipline (including the
`start_supervised!` / monitor-for-DOWN rules and hot-path floor/ceiling guards) is the Testing
section of `AGENTS.md`.

## 4. Where things live

`lib/fathom/` is the platform; `lib/fathom_web/` is the dashboard + `/api` control plane;
`lib/mix/tasks/` holds the `mix fathom.*` operator tasks; `config/` is the config; `docs/` is the
reference set; `deploy/` holds the LB config, the Docker eval stack, and the chaos rig; `scripts/`
holds the bench harness and the commit gate. For which module owns which subsystem, read the
component bullets in [`AGENTS.md`](AGENTS.md) and the "how it works" stories indexed by
[`docs/README.md`](docs/README.md).

## 5. Opening a change

- Branch off `main`; keep commits in logical units.
- Run `mix precommit` (or `scripts/commit_with_bench.sh` for hot-path work) green before committing.
- Write a clear commit message describing *why*; end it with the `Co-Authored-By` trailer if a tool
  helped. Push immediately.
- **Adding or changing a subsystem?** Also add/update its `docs/*.md` "how it works" story — match
  the shape of the existing built-engine docs (problem → constraint → mechanism → safety → the honest
  limit → one-line summary), ground every claim in the code, and link it from its `AGENTS.md` bullet
  and [`docs/README.md`](docs/README.md).
- **Adding a runtime env var?** Document it in [`docs/configuration.md`](docs/configuration.md) — a
  drift test (`test/fathom/configuration_doc_test.exs`) fails the build otherwise.
