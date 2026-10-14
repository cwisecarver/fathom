defmodule Fathom.Shard.TouchProvenanceTest do
  @moduledoc """
  A clean visitor's steal-touch does not make the previous holder's acked local copy a "fork"
  (expert review 2026-10-12 #10).

  ## The defect

  Holder A fails its drop flush: it keeps its local copy (sidecar E0, acked-but-unflushed writes)
  and leaves a released marker (d80e554). Peer B's acquire steals the marker, and the steal's
  `touch_object` rotates the data object E0 -> E1 with a content-identical touch (the zombie-PUT
  fence). B serves reads and drops CLEAN. When A reopens, its steal touches again, E1 -> E2.
  `Fork` sees sidecar E0 against store E1 (or E2) with no touch of A's own explaining E0: verdict
  `:fork`. A's acked local copy is quarantined to `.forked.<ts>` and the OLDER stored object serves.

  This test used to HOLD A's touch until A's fork check had read the store: the touch then
  alternated copy forms, so A's second touch of identical bytes went E1 -> E0, and a fork check
  that happened to read after it saw E0 == sidecar and said `:match` by luck. Since expert review
  2026-10-12 N1 every touch reaches an etag the key never had, so that ordering no longer exists
  and the hold — which was only a workaround for the 2-cycle — is gone.

  ## The fix being pinned

  Not "skip the touch" (that reopens the zombie-PUT window the touch closes). B's CLEAN release
  records the touch's provenance in its marker (`touched_from: E0, touched_to: E1`), and A's steal
  chains it (`touch_origin_etag: E0` because the marker's `touched_to` is exactly A's touch source),
  so A's fork check sees "only touches since your sidecar" and keeps its copy warm.

  Driven end-to-end over the real S3 backend against the MD5-faithful `S3EtagStore`, with real
  coordinators for B and A, both liveness modes (mode asserted).
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore
  alias Filo.{Stmt, StmtResult}

  @modes [:legacy, :heartbeat]

  setup do
    shard = "touchprov_#{System.unique_integer([:positive])}"

    for k <- [:shard_storage, S3, :shard_idle_ms, :replication_enabled] do
      prev = Application.get_env(:fathom, k)

      on_exit(fn ->
        if is_nil(prev),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, prev)
      end)
    end

    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    # The marker and its claim exist only while replicating (d80e554, expert review 2026-10-12 #3).
    Application.put_env(:fathom, :replication_enabled, true)

    on_exit(fn ->
      Shards.drain(shard, 2_000)
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  defp build_db!(rows) do
    path = Path.join(System.tmp_dir!(), "tp_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    for r <- rows, do: :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{r}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end

  defp use_s3!(store) do
    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    Application.put_env(:fathom, :shard_storage, S3)
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp lock(store, shard),
    do: store |> S3EtagStore.body_of("#{shard}.lock") |> Jason.decode!()

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  # `renew?` (expert review 2026-10-13 N2): in LEGACY mode B's coordinator renews its lease while
  # it serves, and the renewed lease used to lose the steal's touch keys, so B's clean release
  # attached no provenance and A's acked copy read as a fork. Renewal does not exist in heartbeat
  # mode (no per-shard renew PUTs), so that combination is not a case.
  for {mode, renew?} <- [{:legacy, false}, {:legacy, true}, {:heartbeat, false}] do
    test "#{mode} mode#{if renew?, do: ", B renewed its lease"}: A's acked local copy survives B's clean visit",
         %{shard: shard} do
      base = build_db!(["base"])

      # A failed its drop flush: a released marker carrying A's claim (Storage.released_marker/2).
      marker = Storage.encode_lease(Storage.released_marker(%{epoch: 3}, 5))

      store =
        start_supervised!(
          {Agent,
           fn -> S3EtagStore.initial(%{"#{shard}.db" => base, "#{shard}.lock" => marker}) end}
        )

      use_s3!(store)
      set_mode!(unquote(mode))
      e0 = S3EtagStore.etag_of(store, "#{shard}.db")

      # A's local copy: the stored bytes plus an acked write that never became durable, provenance
      # E0. Held aside while B runs on this same data dir (B is "another node").
      a_bytes =
        Path.join(System.tmp_dir!(), "tp_a_#{System.unique_integer([:positive])}.db")
        |> tap(&File.write!(&1, base))

      {:ok, ac} = Connection.open(a_bytes)
      :ok = Connection.exec(ac, "INSERT INTO kv VALUES ('a-acked')")
      :ok = Connection.exec(ac, "PRAGMA wal_checkpoint(TRUNCATE)")
      Connection.close(ac)
      on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(a_bytes <> s) end)

      capture_log(fn ->
        # B: steals A's marker (touch E0 -> E1), serves a read, drops CLEAN.
        {:ok, b} = Shards.ensure(shard)
        assert_mode!(b, unquote(mode))
        assert :sys.get_state(b).lease[:touch_pre_etag] == e0, "B's steal must touch from E0"
        refute Shard.dirty?(b)

        if unquote(renew?) do
          assert is_nil(:sys.get_state(b).acquire_gen), "N2 is a legacy-mode defect"
          before = :sys.get_state(b).lease.lock_etag
          send(b, :renew_lease)
          renewed = renewed_lease!(b, before, 200)
          assert renewed[:touch_post_etag], "a renew must carry the steal's touch keys"
        end

        ref = Process.monitor(b)
        :ok = Shards.drain(shard, 5_000)
        assert_receive {:DOWN, ^ref, :process, ^b, _}, 5_000
      end)

      e1 = S3EtagStore.etag_of(store, "#{shard}.db")
      assert e1 != e0, "the fixture needs B's touch to have rotated the etag"
      body = lock(store, shard)
      assert body["owner"] == Storage.released_owner()
      assert body["touched_from"] == e0 and body["touched_to"] == e1

      # A reopens with its acked local copy.
      local = Path.join(Shard.data_dir(), "#{shard}.db")
      File.mkdir_p!(Shard.data_dir())
      File.cp!(a_bytes, local)
      File.write!(local <> ".etag", e0)

      capture_log(fn ->
        {:ok, conn} = ShardExecutor.open(shard)

        assert {:ok, %StmtResult{rows: [["a-acked"]]}} =
                 ShardExecutor.execute(conn, stmt("SELECT v FROM kv WHERE v = 'a-acked'")),
               "A's acked local write must be served, not quarantined behind the older object"

        :ok = ShardExecutor.close(conn)
        # Stop while the store double is still up (on_exit runs after it is gone).
        Shards.drain(shard, 5_000)
      end)

      assert Path.wildcard(local <> ".forked.*") == [],
             "only content-identical touches happened since A's sidecar: that is not a fork"

      refute S3EtagStore.etag_of(store, "#{shard}.db") in [e0, e1],
             "A's touch must reach an etag the key never had (pre-N1 it returned to E0)"
    end
  end

  test "a write since the marker's touch breaks the chain: still a fork", %{shard: shard} do
    base = build_db!(["base"])
    other = build_db!(["base", "peer-write"])
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{"#{shard}.db" => base}) end})
    use_s3!(store)
    e0 = S3EtagStore.etag_of(store, "#{shard}.db")

    # A marker claiming B touched E0 -> X, but the object was since REWRITTEN (a peer's flush), so
    # A's touch sources something else: the chain must not link.
    S3EtagStore.set_body(store, "#{shard}.db", other)

    marker =
      Storage.released_marker(
        %{epoch: 3, touch_pre_etag: e0, touch_post_etag: "x-touched"}
        |> Storage.with_clean_touch(),
        5
      )

    assert marker.touched_from == e0

    Agent.update(
      store,
      &put_in(&1, [:objects, "#{shard}.lock"], obj(Storage.encode_lease(marker)))
    )

    local = Path.join(Shard.data_dir(), "#{shard}.db")
    File.mkdir_p!(Shard.data_dir())
    File.write!(local, base)
    File.write!(local <> ".etag", e0)

    capture_log(fn ->
      {:ok, conn} = ShardExecutor.open(shard)

      assert {:ok, %StmtResult{rows: [["peer-write"]]}} =
               ShardExecutor.execute(conn, stmt("SELECT v FROM kv WHERE v = 'peer-write'"))

      :ok = ShardExecutor.close(conn)
      Shards.drain(shard, 5_000)
    end)

    assert Path.wildcard(local <> ".forked.*") != []
  end

  # Poll the coordinator until the renew task's reply has been folded into state.lease.
  defp renewed_lease!(_b, _before, 0), do: flunk("the renew never landed")

  defp renewed_lease!(b, before, n) do
    st = :sys.get_state(b)

    if st.renew_task == nil and st.lease.lock_etag != before do
      st.lease
    else
      Process.sleep(10)
      renewed_lease!(b, before, n - 1)
    end
  end

  defp obj(body), do: %{body: body, form: :single, meta: %{}}
end
