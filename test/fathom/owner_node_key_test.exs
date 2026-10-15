defmodule Fathom.OwnerNodeKeyTest do
  @moduledoc """
  The lease owner carries NODE_KEY, not `node()` (expert review 2026-10-15 #17).

  Symptom: `Heartbeat.owner/0` was `"<node()>#<nonce>"` while the rebalancer, the LB, RemoteFlush and
  the command channel address nodes by `Rebalancer.node_key/0` (`:node_key` / NODE_KEY). Wherever the
  two differ (the chaos rig; any real deployment that sets NODE_KEY) three consumers silently
  no-opped: `HandoffJob.settle_or_revert` never recognised the target as the holder (a handoff that
  landed was reverted, tenant 503s), the previous-owner exclusion in recovery / promote-on-open
  compared a `node()` name against a key, and a B2 workaround claimed commands under both names.

  Invariant: with `:node_key` != `node()`, the owner string's key equals `node_key/0`, and every
  consumer that maps an owner to a node agrees. Each test asserts that precondition took.
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  alias Fathom.Rebalancer
  alias Fathom.Rebalancer.{HandoffJob, Overrides}
  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Fathom.Shards

  @modes [:legacy, :heartbeat]

  setup do
    key = "keyed-#{System.unique_integer([:positive])}"
    prev = Application.get_env(:fathom, :node_key)
    Application.put_env(:fathom, :node_key, key)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :node_key, prev),
        else: Application.delete_env(:fathom, :node_key)
    end)

    refute Rebalancer.node_key() == to_string(node()),
           "fixture: NODE_KEY must differ from node() or this file proves nothing"

    %{key: key}
  end

  describe "the owner string" do
    test "carries node_key, parses back to it, and ignores migrators", %{key: key} do
      assert Heartbeat.owner() =~ ~r/\A#{key}#/
      assert Rebalancer.owner_node_key(Heartbeat.owner()) == key
      assert Rebalancer.owner_node_key("migrator@a@b") == nil
      assert Rebalancer.owner_node_key(nil) == nil
    end
  end

  describe "recovery previous-owner exclusion" do
    @pos %{wal_ordinal: 4, lineage: 7, torn: false}

    test "self is excluded by key (a node() comparison missed it)" do
      answers = [{{"p1", "h", 1}, {:answered, @pos}}, {{"p2", "h", 2}, {:answered, @pos}}]

      # own table (1) is dropped as the previous owner; the two peers count.
      assert Recovery.answered(answers, @pos, previous_owner: Heartbeat.owner()) == 2
      assert Recovery.answered(answers, @pos, previous_owner: nil) == 3
    end

    test "a peer previous owner is excluded by its key" do
      answers = [{{"p1", "h", 1}, {:answered, @pos}}, {{"p2", "h", 2}, {:answered, @pos}}]
      assert Recovery.answered(answers, nil, previous_owner: "p1#boot") == 2
      # exact, not prefix or host-suffix
      assert Recovery.answered(answers, nil, previous_owner: "p10#boot") == 3
      refute Recovery.previous_owner?("a@p1#boot", "p1")
    end
  end

  describe "RemoteFlush" do
    for mode <- @modes do
      test "a lease held by THIS node's key is local, not a remote command (#{mode})" do
        Application.put_env(:fathom, :command_poller, true)
        Application.put_env(:fathom, :remote_flush_ms, 200)

        on_exit(fn ->
          Application.delete_env(:fathom, :command_poller)
          Application.delete_env(:fathom, :remote_flush_ms)
        end)

        id = "ownerkey-flush-#{System.unique_integer([:positive])}"
        set_mode!(unquote(mode))
        on_exit(fn -> cleanup(id) end)

        {:ok, pid, _ref, _path} = Shards.checkout(id)
        assert_mode!(pid, unquote(mode))
        assert {:held, owner} = Storage.lease_holder(id)
        assert Rebalancer.owner_node_key(owner) == Rebalancer.node_key()

        # Unfixed: owner key != node() so this node read ITSELF as remote, issued a flush command
        # nobody answered, and returned {:error, :remote_holder_unflushed} after the await.
        assert :ok = Fathom.Shards.RemoteFlush.flush(id)
      end
    end
  end

  describe "HandoffJob settle_or_revert" do
    setup %{key: key} do
      map_path =
        Path.join(System.tmp_dir!(), "fathom_lbmap_#{System.unique_integer([:positive])}.conf")

      keys = [
        :shard_idle_ms,
        :lb_map_path,
        :lb_backends,
        :handoff_warm_timeout_ms,
        :handoff_drain_timeout_ms
      ]

      prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})
      Application.put_env(:fathom, :shard_idle_ms, 600_000)
      Application.put_env(:fathom, :lb_map_path, map_path)
      Application.put_env(:fathom, :lb_backends, %{key => "#{key}:8080"})
      Application.put_env(:fathom, :handoff_warm_timeout_ms, 50)
      Application.put_env(:fathom, :handoff_drain_timeout_ms, 50)

      on_exit(fn ->
        for {k, v} <- prev do
          if v == nil,
            do: Application.delete_env(:fathom, k),
            else: Application.put_env(:fathom, k, v)
        end

        File.rm(map_path)
      end)

      :ok
    end

    for mode <- @modes do
      test "the pin is kept when the target (by NODE_KEY) holds the lease (#{mode})", %{key: key} do
        shard = "ownerkey-handoff-#{System.unique_integer([:positive])}"
        set_mode!(unquote(mode))
        on_exit(fn -> cleanup(shard) end)

        {:ok, pid, _ref, _path} = Shards.checkout(shard)
        assert_mode!(pid, unquote(mode))
        assert {:held, owner} = Storage.lease_holder(shard)
        assert String.starts_with?(owner, key <> "#")

        args = %{
          "shard_id" => shard,
          "from_node" => "source@elsewhere",
          "to_node" => key,
          "q_per_s" => 500.0
        }

        assert :ok = perform_job(HandoffJob, args, attempt: 3)
        o = Overrides.for_shard(shard)
        assert o.failed_at == nil, "a handoff that landed was reverted: owner/key mismatch"
        assert o.pinned_node == key
      end
    end
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp cleanup(id) do
    Shards.stop(id)

    for dir <- [Fathom.Shard.data_dir(), Storage.Local.dir()],
        path <- Path.wildcard(Path.join(dir, "#{id}*")),
        do: File.rm(path)
  end
end
