# :s3 tests hit a live S3-compatible store (MinIO) and are excluded by default.
# Run them with: mix test --include s3  (see test/fathom/shard_storage_s3_test.exs).
# :bench tests are hot-path floor/ceiling guards (seconds of setup each) and are
# excluded by default. Run with: mix test --include bench  (see test/fathom/bench_test.exs).

# Clear stale storage DR artifacts from prior runs (#6). Deleted-tenant tests write durable
# `tombstones/<id>` (and token tests `tokenfloors/<id>`) markers that are, by design, never swept —
# so they accumulate across runs. `System.unique_integer` resets per BEAM run, so a leftover
# `tombstones/ten_<N>` from a previous run collides with a fresh `ten_<N>` this run, and the
# boot-time storage-union then falsely tombstones an unrelated test's tenant. Clean the dirs and drop
# the entries the app's boot already unioned in, so every run starts from a clean re-mint gate.
remote_dir =
  Application.get_env(:fathom, Fathom.Shard.Storage.Local, [])[:dir] ||
    Path.join(System.tmp_dir!(), "fathom_remote_test")

for sub <- ["tombstones", "tokenfloors"], do: File.rm_rf(Path.join(remote_dir, sub))

try do
  :ets.delete_all_objects(Fathom.Tenants.Tombstones)
rescue
  ArgumentError -> :ok
end

ExUnit.start(exclude: [:s3, :bench])
Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)
