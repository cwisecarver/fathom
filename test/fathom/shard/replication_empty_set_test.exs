defmodule Fathom.Shard.ReplicationEmptySetTest do
  @moduledoc """
  Expert review 2026-11-07 #17: a tenant commit was acked `:ok` with ZERO followers when the
  published shipper set was empty.

  `Session.ship_planned/5` reads "no plans" for both "every follower already holds the extent" and
  "there are no followers", and answered the first with an ack — so during the windows in which
  `Fleet.shippers/0` is `[]` while the Session tree is up (`Fleet.init/1`'s `publish([])` before
  `Membership` publishes; `Membership.terminate/2`'s retraction on an orderly stop) a write held on
  this node's disk alone was told it was quorum-durable.

  ## The policy

  With replication on, `Fleet.validate_quorum!/0` refuses to boot with no followers and
  `Membership` refuses any set of `q` or fewer, so an empty set is never a configuration, only a
  transient window: the tenant write is refused (retryable 503), not acked. A set with every
  follower current is a different state and still acks (pinned below).

  Both liveness modes run; the ack path differs.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Heartbeat
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shards
  import Fathom.Test.ReplicationCase

  @modes [:legacy, :heartbeat]

  setup do
    id = "repl_empty_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replempty_#{System.unique_integer([:positive])}")
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
      test "a tenant commit is refused while the published shipper set is empty", ctx do
        %{id: id, root: root} = ctx
        set_mode!(unquote(mode))
        followers = start_followers!(root, 3, "es")
        enable_replication!(followers, connected: :all)

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

        # Every follower now holds everything: a commit with nothing left to ship still acks. This
        # is the "all followers current" state the empty-set refusal must not be confused with.
        assert :ok = Session.commit(id, wal, coordinator)

        running = Fleet.running()
        assert length(running) == 3, "precondition: the fleet did not publish its followers"

        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2)", [])

        # The window: `Fleet.init/1` published `[]` and `Membership` has not republished.
        Fleet.publish([])

        # NO `on_exit(Fleet.publish(running))` here. ExUnit terminates `start_supervised!` children
        # BEFORE it runs `on_exit` callbacks, so by then `Membership.terminate/2` has already
        # retracted the set and a republish would LEAVE THREE DEAD SHIPPER NAMES in `:persistent_term`
        # for every later test (seed 169289: `RecoveryPrefetchTest` then asked 3 dead followers and
        # refused its open with `UnderQuorum` 0-of-2). The in-test republish below, and `Membership`'s
        # retraction when the Fleet stops, are the whole cleanup.
        assert Fleet.shippers() == [], "precondition: the published set is not empty"

        assert {:error, {:no_quorum, :impossible}} = Session.commit(id, wal, coordinator),
               "a write held on this node alone was acked quorum-durable (pre-fix: :ok, no push)"

        # The set comes back and the same Session acks again.
        Fleet.publish(running)
        assert :ok = Session.commit(id, wal, coordinator)
      end
    end
  end
end
