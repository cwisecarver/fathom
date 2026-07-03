# :s3 tests hit a live S3-compatible store (MinIO) and are excluded by default.
# Run them with: mix test --include s3  (see test/fathom/shard_storage_s3_test.exs).
# :bench tests are hot-path floor/ceiling guards (seconds of setup each) and are
# excluded by default. Run with: mix test --include bench  (see test/fathom/bench_test.exs).
ExUnit.start(exclude: [:s3, :bench])
Ecto.Adapters.SQL.Sandbox.mode(Fathom.Repo, :manual)
