defmodule Fathom.Shard.Storage.S3TouchAbaTest do
  @moduledoc """
  Every steal-touch moves the data object to an etag that key has NEVER had (expert review
  2026-10-12 N1).

  ## The defect

  The touch used to alternate copy forms — a single-form (MD5) etag was touched with a one-part
  multipart self-copy, a multipart one with a plain self-copy. Each touch moved the etag, but for
  identical bytes only two etags exist, so two consecutive steals went E0 -> E1 -> E0. A zombie
  that flushed at E0 and stalled across both steals then had its late `If-Match: E0` PUT
  ACCEPTED: an unfenced write past the fence, clobbering the live owner's object.

  ## What is pinned

    * two steals then the zombie's E0 flush is refused (`:superseded`), for a small object (the
      nonce-in-bytes touch) and a large one (the random-boundary two-part copy);
    * three consecutive touches never repeat an etag, at both sizes;
    * a touched object still decodes, on every read path, to the same database with the same
      `x-amz-meta-fathom-md5` — including one that was stored `:none` or `:zlib` and was
      re-encoded by the touch;
    * the end-to-end version with a real coordinator as the zombie, in both liveness modes.

  Driven against `Fathom.Test.S3EtagStore`, whose etags are content-derived like real S3 and
  whose multipart model honours part ranges and the 5 MiB part minimum.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Fathom.Shard.Storage.{Codec, HeartbeatCache, S3}
  alias Fathom.Test.S3EtagStore
  alias Filo.{Stmt, StmtResult}

  @md5_meta "x-amz-meta-fathom-md5"

  setup do
    shard = "touchaba_#{System.unique_integer([:positive])}"

    for k <- [:shard_storage, S3, :shard_idle_ms, :steal_margin_ms] do
      prev = Application.get_env(:fathom, k)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, prev)
      end)
    end

    Application.put_env(:fathom, :steal_margin_ms, 0)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    HeartbeatCache.reset()

    on_exit(fn ->
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
      for f <- Path.wildcard(Path.join(System.tmp_dir!(), "aba_#{shard}*")), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  defp use_s3!(store, plug \\ nil) do
    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: plug || fn conn -> S3EtagStore.serve(conn, store) end
    )

    Application.put_env(:fathom, :shard_storage, S3)
  end

  defp start_store(objects \\ %{}) do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(objects) end})
    use_s3!(store)
    store
  end

  defp tmp_file(shard, bytes) do
    path = Path.join(System.tmp_dir!(), "aba_#{shard}_#{System.unique_integer([:positive])}")
    File.write!(path, bytes)
    path
  end

  # A real (tiny) SQLite database, so the read-path checks can open what they pull.
  defp build_db!(shard, rows) do
    path = tmp_file(shard, "")
    File.rm!(path)
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "PRAGMA user_version = 7")
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    for r <- rows, do: :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{r}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end

  # Large and incompressible (random bytes, the worst case for the codec), so the STORED object
  # stays above the split-touch floor (6 MiB) even after zstd.
  defp large_payload, do: :crypto.strong_rand_bytes(7 * 1024 * 1024 + 123)

  # Makes the CURRENT lock holder stealable: its lock is expired and so is its node heartbeat (if
  # it has one), the state of a zombie stalled past its lease.
  defp expire_holder!(store, shard) do
    {:ok, lease} = Storage.decode_lease(S3EtagStore.body_of(store, "#{shard}.lock"))
    past = Storage.now_ms() - 120_000

    S3EtagStore.set_body(
      store,
      "#{shard}.lock",
      Storage.encode_lease(%{lease | expires_at_ms: past})
    )

    S3EtagStore.set_body(
      store,
      "heartbeats/" <> URI.encode_www_form(lease.owner),
      Storage.encode_heartbeat(%{owner: lease.owner, expires_at_ms: past})
    )

    HeartbeatCache.reset()
  end

  defp steal!(store, shard, owner) do
    expire_holder!(store, shard)
    assert {:ok, %{took_over: true} = lease} = S3.acquire_lease(shard, owner, 30_000)
    lease
  end

  for {size, payload} <- [small: :db, large: :large] do
    test "#{size} object: two steals then the zombie's original-etag flush is REFUSED",
         %{shard: shard} do
      store = start_store()

      bytes =
        if unquote(payload) == :db, do: build_db!(shard, ["base"]), else: large_payload()

      # Z owns the shard and flushes: the object is at E0, which Z keeps as its fence.
      {:ok, _} = S3.acquire_lease(shard, "z@n1#1", 30_000)
      {:ok, e0, _} = S3.flush(shard, tmp_file(shard, bytes), nil)

      a = steal!(store, shard, "a@n2#1")
      b = steal!(store, shard, "b@n3#1")
      assert a.touch_pre_etag == e0
      assert b.touch_pre_etag == a.touch_post_etag

      e2 = S3EtagStore.etag_of(store, "#{shard}.db")

      # Z's late, stalled flush — it passed its own fence check long ago and fences on E0.
      zombie = tmp_file(shard, bytes <> "late acked write")

      assert {:error, :superseded} = S3.flush(shard, zombie, e0),
             "a zombie flush fenced on the pre-steal etag must be refused after two steals"

      assert S3EtagStore.etag_of(store, "#{shard}.db") == e2, "the live object was clobbered"
      refute e2 == e0, "two touches of identical bytes returned the object to E0 (N1's ABA)"

      # And the steals moved no CONTENT: a pull still yields the flushed bytes.
      out = tmp_file(shard, "")
      assert {:ok, ^e2} = S3.pull(shard, out)
      assert File.read!(out) == bytes
    end

    test "#{size} object: three consecutive touches never repeat an etag", %{shard: shard} do
      store = start_store()

      bytes =
        if unquote(payload) == :db, do: build_db!(shard, ["base"]), else: large_payload()

      {:ok, _} = S3.acquire_lease(shard, "z@n1#1", 30_000)
      {:ok, e0, _} = S3.flush(shard, tmp_file(shard, bytes), nil)
      md5 = S3EtagStore.meta_of(store, "#{shard}.db")[@md5_meta]

      etags =
        for owner <- ["a@n2#1", "b@n3#1", "c@n4#1"] do
          steal!(store, shard, owner)
          S3EtagStore.etag_of(store, "#{shard}.db")
        end

      assert length(Enum.uniq([e0 | etags])) == 4,
             "a touch repeated an etag: #{inspect([e0 | etags])}"

      # The path taken is the one the size calls for: the small object is rewritten (single-form
      # MD5 etag of new bytes), the large one is split into a two-part multipart copy.
      for e <- etags do
        if unquote(size) == :large,
          do: assert(e =~ ~r/-2"$/, "large touch must be a two-part copy: #{e}"),
          else: refute(e =~ "-", "small touch must be a single-PUT rewrite: #{e}")
      end

      assert S3EtagStore.meta_of(store, "#{shard}.db")[@md5_meta] == md5,
             "the integrity digest (of the UNCOMPRESSED database) must not change across touches"

      assert S3EtagStore.plain_body_of(store, "#{shard}.db") == bytes
    end
  end

  test "the split boundary keeps part 1 at S3's minimum and part 2 non-empty" do
    min = 5 * 1024 * 1024

    for size <- [min + 1024 * 1024, min + 1024 * 1024 + 1, 64 * 1024 * 1024] do
      for _ <- 1..200 do
        cut = S3.split_point(size)
        assert cut >= min and cut < size
      end
    end
  end

  # Every S3 read decodes through the same two places (the streamed download, and the ranged
  # header read), but each entry point is exercised here on a nonced object anyway: pull (cold
  # open, export, restore drill), object_user_version (fork stamping), fork_shard, snapshot +
  # pull_snapshot, and a retained version restored back.
  for seed_enc <- [:zstd, :none, :zlib] do
    test "#{seed_enc}-stored object: after two touches every read path decodes the same database",
         %{shard: shard} do
      db = build_db!(shard, ["base", "more"])
      store = start_store()
      {:ok, _} = S3.acquire_lease(shard, "z@n1#1", 30_000)
      md5 = Base.encode16(:crypto.hash(:md5, db), case: :lower)

      case unquote(seed_enc) do
        :zstd ->
          {:ok, _, _} = S3.flush(shard, tmp_file(shard, db), nil)

        :none ->
          # A legacy raw object with no digest metadata: verified against its MD5 etag.
          S3EtagStore.set_body(store, "#{shard}.db", db)

        :zlib ->
          S3EtagStore.set_body(store, "#{shard}.db", :zlib.compress(db))

          Agent.update(store, fn s ->
            put_in(s, [:objects, "#{shard}.db", :meta], %{
              Codec.meta_header() => "zlib",
              @md5_meta => md5
            })
          end)
      end

      steal!(store, shard, "a@n2#1")
      steal!(store, shard, "b@n3#1")

      meta = S3EtagStore.meta_of(store, "#{shard}.db")
      assert meta[Codec.meta_header()] == "zstd", "the touch leaves the object zstd-encoded"
      assert meta[@md5_meta] == md5, "the digest is of the database, unchanged by the touch"
      refute S3EtagStore.body_of(store, "#{shard}.db") == db

      pulled = tmp_file(shard, "")
      assert {:ok, _} = S3.pull(shard, pulled)
      assert File.read!(pulled) == db

      assert {:ok, 7} = S3.object_user_version(shard)

      fork = "#{shard}fork"
      assert :ok = S3.fork_shard(shard, fork)
      forked = tmp_file(shard, "")
      assert {:ok, _} = S3.pull(fork, forked)
      assert File.read!(forked) == db

      assert :ok = S3.snapshot(shard, "snap1")
      snap = tmp_file(shard, "")
      assert {:ok, _} = S3.pull_snapshot(shard, "snap1", snap)
      assert File.read!(snap) == db

      assert :ok = S3.retain(shard, 1)
      assert :ok = S3.restore(shard, 1, S3EtagStore.etag_of(store, "#{shard}.db"))
      restored = tmp_file(shard, "")
      assert {:ok, _} = S3.pull(shard, restored)
      assert File.read!(restored) == db
    end
  end

  test "a raw object whose bytes do not match its digest is not re-stamped by the touch",
       %{shard: shard} do
    store = start_store()
    {:ok, _} = S3.acquire_lease(shard, "z@n1#1", 30_000)

    # A raw object claiming a digest its bytes do not have (torn or foreign). Re-encoding it would
    # give it a fresh, matching digest and launder it into something every pull then accepts.
    S3EtagStore.set_body(store, "#{shard}.db", "torn bytes")

    Agent.update(store, fn s ->
      put_in(s, [:objects, "#{shard}.db", :meta], %{
        @md5_meta => Base.encode16(:crypto.hash(:md5, "real bytes"), case: :lower)
      })
    end)

    stamped = S3EtagStore.etag_of(store, "#{shard}.db")
    expire_holder!(store, shard)
    assert {:error, _} = S3.acquire_lease(shard, "a@n2#1", 30_000)
    assert S3EtagStore.etag_of(store, "#{shard}.db") == stamped
    assert S3EtagStore.body_of(store, "#{shard}.db") == "torn bytes"
  end

  # --- end to end: a real coordinator is the zombie ---

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  # Holds the coordinator's fenced data PUT (it has passed every fence check of its own) until the
  # test says go — the stalled-zombie window.
  defp stalling_plug(store, gate, shard) do
    test_pid = self()
    key = "#{shard}.db"

    fn conn ->
      fenced_put? =
        conn.method == "PUT" and String.ends_with?(conn.request_path, key) and
          Plug.Conn.get_req_header(conn, "if-match") != [] and
          Plug.Conn.get_req_header(conn, "x-amz-copy-source") == [] and
          Agent.get(gate, & &1)

      if fenced_put? do
        Agent.update(gate, fn _ -> false end)
        send(test_pid, {:stalled, self()})

        receive do
          :go -> :ok
        after
          10_000 -> :ok
        end
      end

      S3EtagStore.serve(conn, store)
    end
  end

  for mode <- [:legacy, :heartbeat] do
    test "#{mode} mode: a coordinator stalled in its flush across two steals cannot land it",
         %{shard: shard} do
      base = build_db!(shard, ["base"])
      store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{"#{shard}.db" => base}) end})
      gate = start_supervised!({Agent, fn -> false end}, id: :gate)
      use_s3!(store, stalling_plug(store, gate, shard))
      set_mode!(unquote(mode))

      capture_log(fn ->
        {:ok, conn} = ShardExecutor.open(shard)

        assert {:ok, %StmtResult{}} =
                 ShardExecutor.execute(conn, %Stmt{
                   sql: "INSERT INTO kv VALUES ('zombie-write')",
                   args: []
                 })

        :ok = ShardExecutor.close(conn)
        {:ok, z} = Shards.ensure(shard)
        assert_mode!(z, unquote(mode))
        assert Shard.dirty?(z)
        e0 = S3EtagStore.etag_of(store, "#{shard}.db")

        Agent.update(gate, fn _ -> true end)
        flush = Task.async(fn -> Shards.flush(shard) end)
        assert_receive {:stalled, put_pid}, 5_000

        steal!(store, shard, "a@n2#1")
        steal!(store, shard, "b@n3#1")
        e2 = S3EtagStore.etag_of(store, "#{shard}.db")

        send(put_pid, :go)
        _ = Task.await(flush, 15_000)

        assert S3EtagStore.etag_of(store, "#{shard}.db") == e2,
               "the zombie's flush landed past two steals"

        refute S3EtagStore.plain_body_of(store, "#{shard}.db") =~ "zombie-write"
        refute e2 == e0, "two touches returned the object to the zombie's fence etag (N1)"
        Shards.drain(shard, 5_000)
      end)
    end
  end
end
