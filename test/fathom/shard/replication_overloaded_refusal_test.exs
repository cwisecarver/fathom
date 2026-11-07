defmodule Fathom.Shard.ReplicationOverloadedRefusalTest do
  @moduledoc """
  Expert review 2026-11-07 #44: a local `:overloaded` refusal left a PHANTOM in-flight expectation,
  so the shard read "busy" and every commit burned its whole deadline.

  ## The defect

  `Session.send_round/7` records `inflight[F]` for every planned follower BEFORE sending. When the
  node byte budget (`Budget.reserve/2`, `:replication_max_queue_bytes`) is exhausted,
  `Shipper.push/2` answers `{:reject, shard, :overloaded, 0}` to the caller itself and sends
  nothing. `reconcile/2` KEPT the entry, on the reasoning that `:overloaded` means "the follower is
  still on the hook for an EARLIER push" — true for `:already_in_flight`, false when this very
  round created the entry. The next commit then saw `map_size(inflight) != 0` with no free
  followers and waited out its deadline for replies that were never coming
  (`await_stragglers_if_short/3`), failing `{:no_quorum, _}` for up to `2 * timeout()` after the
  overload had ended.

  ## What is pinned

  An `:overloaded` refusal of a push this Session sent as the follower's FIRST outstanding push
  leaves no expectation behind, and the next commit pushes at once. The refusal of a SECOND push to
  a follower that still owes an earlier reply keeps the entry (that reply can still be reconciled:
  expert review 2026-08-20 #27) — covered by `replication_seed_test.exs` and not repeated here.

  The overload is the real one: `:replication_max_queue_bytes` of 1 makes every real `Shipper`
  refuse locally. Both liveness modes run; the ack path differs.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Heartbeat
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shards
  alias Fathom.Test.Env
  import Fathom.Test.ReplicationCase

  @modes [:legacy, :heartbeat]

  setup do
    id = "repl_ovl_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replovl_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    on_exit(fn ->
      Session.stop(id)
      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    owner = "n_#{System.unique_integer([:positive])}@test"
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000, owner: owner})
    _ = :sys.get_state(hb)
    :ok
  end

  defp assert_mode!(:legacy, coordinator),
    do: assert(is_nil(Fathom.Shard.acquire_gen(coordinator)), "expected LEGACY liveness")

  defp assert_mode!(:heartbeat, coordinator),
    do: refute(is_nil(Fathom.Shard.acquire_gen(coordinator)), "expected HEARTBEAT liveness")

  for mode <- @modes do
    describe "#{mode} mode" do
      test "a refused first push leaves no phantom in-flight and the next commit ships at once",
           %{id: id, root: root} do
        set_mode!(unquote(mode))
        followers = start_followers!(root, 3, "ov")

        # A short timeout so the PRE-fix failure (a commit waiting out its deadline for replies that
        # never come) is quick; a huge catch-up interval so no background cycle touches the state.
        enable_replication!(followers, connected: :all, timeout: 1_500, catchup: 600_000)

        {:ok, coordinator, ref, path} = Shards.checkout(id)
        on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
        {:ok, conn} = Connection.open(path)
        on_exit(fn -> Connection.close(conn) end)
        assert_mode!(unquote(mode), coordinator)

        {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
        wal = path <> "-wal"
        for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)
        assert :ok = Session.commit(id, wal, coordinator)

        # The FIRST commit of a fresh lineage seeds every follower directly (`Session.seed_first/4`)
        # and returns at the QUORUM, so the third follower's seed can still be in flight here under
        # load. That seed's follower is then `seeding` (not pushed to, and `inflight` is not idle)
        # while the overload commit below runs, and the assertion that follows read a Session that
        # was still settling the first commit: seen twice in full-suite runs as `[overloaded: 0,
        # seeding: 0]` (the log line of a seed in flight) and `inflight != %{}`, never in isolation.
        # Wait for it, so the test measures the overload and not the first commit's stragglers.
        await_no_seeds!(id)

        assert :sys.get_state(session!(id)).inflight == %{},
               "fixture: not idle after a good commit"

        # The overload: every shipper refuses locally, before anything reaches the socket.
        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2)", [])
        Env.put!(:replication_max_queue_bytes, 1)

        assert {:error, {:no_quorum, _}} = Session.commit(id, wal, coordinator),
               "fixture: the overload did not fail the quorum, so no push was refused locally"

        # Let any refusal the round did not read land, then look at what the Session believes.
        _ = :sys.get_state(session!(id))

        assert :sys.get_state(session!(id)).inflight == %{},
               "a push the local shipper REFUSED is still recorded as in flight (pre-fix: 3 " <>
                 "phantom entries, so the next commit waits out its deadline for replies that " <>
                 "were never coming)"

        # The overload ends. The next commit must not wait on anything: it ships and acks.
        Application.put_env(:fathom, :replication_max_queue_bytes, 1024 * 1024 * 1024)
        t0 = System.monotonic_time(:millisecond)
        assert :ok = Session.commit(id, wal, coordinator)

        assert System.monotonic_time(:millisecond) - t0 < 1_000,
               "the commit after the overload sat on phantom in-flight entries"
      end
    end
  end
end
