defmodule Fathom.Cluster.LegacyFenceTest do
  @moduledoc """
  LEGACY mode end to end — a node running no heartbeat, whose liveness is its per-shard lock TTL
  (expert review 2026-08-01 #30, item 3). This is where #9 and #12 live and neither had an
  end-to-end test.

  ## What legacy mode is, and the trap in testing it

  `Fathom.Shard.Heartbeat` makes liveness O(nodes) instead of O(shards) — a shard's owner is live
  iff its node heartbeat is fresh. When the heartbeat process is DOWN, coordinators degrade to the
  older per-shard renew fence, and `owner_live?/3` falls back to the lock's own TTL
  (`local.ex:490-502`).

  **`config/test.exs` sets `heartbeat_server: false`, so every coordinator in the suite is already
  in legacy mode.** A setup block "forcing" it would be a no-op that makes a test look more
  specific than it is — that exact mistake is recorded in AGENTS.md. So these tests do not force
  the mode; they assert the properties that only hold in it, from the OUTSIDE (a peer's
  `acquire_lease` / `lease_holder`), which is the view that actually matters for single-writer
  safety.

  ## The two findings

  * **#12** — a present-but-STALE heartbeat must not make a healthy, lock-renewing node's whole
    shard set stealable. An owner is dead only when BOTH its heartbeat and its lock TTL have
    lapsed. Before the fix a stale heartbeat alone was enough, so one node's heartbeat hiccup
    handed every shard it owned to a peer.
  * **#9** — a drained coordinator must actually FREE the lock, not merely stop. `flush_then_drop`
    discarded the fence-refreshed lease, so the release used a stale token and silently failed:
    the shard stayed locked with nobody running it, un-openable anywhere until the TTL expired.
  """
  use Fathom.ClusterShardCase, async: false

  alias Fathom.Shard.Storage

  @peer "peer@othernode#1"

  # `Local.heartbeat_path/1` URI-encodes the owner (`peer@othernode#1` → `peer%40othernode%231`),
  # so writing the RAW name produces a file nothing ever reads. That is not hypothetical: the
  # first version of this helper did exactly that, `read_heartbeat/1` returned `:not_found`, and
  # every #12 test below passed through the `:not_found` fallback branch instead of the branch
  # under test. Hence the precondition assertion — a heartbeat fixture that is not actually read
  # makes these tests prove nothing.
  defp put_heartbeat(owner, expires_at_ms) do
    dir = Path.join(Fathom.Shard.Storage.Local.dir(), "heartbeats")
    File.mkdir_p!(dir)

    path = Path.join(dir, URI.encode_www_form(owner))

    File.write!(
      path,
      Storage.encode_heartbeat(%{owner: owner, expires_at_ms: expires_at_ms})
    )

    on_exit(fn -> File.rm(path) end)

    assert {:ok, %{owner: ^owner}} = Storage.read_heartbeat(owner),
           "the heartbeat fixture is not readable by the backend — these tests would silently " <>
             "exercise the no-heartbeat fallback instead of the stale-heartbeat branch"
  end

  describe "#12 — a stale heartbeat alone does not make a shard stealable" do
    test "a fresh lock TTL keeps the shard held even though the owner's heartbeat has lapsed",
         %{shard: shard} do
      # The owner published a heartbeat and then stopped renewing it (a GC pause, a slow
      # scheduler, a heartbeat process restart) — but it is healthily renewing THIS shard's lock.
      put_heartbeat(@peer, Storage.now_ms() - Storage.steal_margin_ms() - 60_000)
      put_raw_lock(shard, @peer, 4, Storage.now_ms() + 60_000)

      assert {:error, {:held, @peer, _}} = Storage.acquire_lease(shard, "me@node#1", 30_000),
             "a stale heartbeat alone made a lock-renewing owner stealable — one heartbeat " <>
               "hiccup would hand away every shard that node owns"

      assert {:held, @peer} = Storage.lease_holder(shard)
    end

    test "both lapsed IS stealable — the fix must not wedge a genuinely dead owner",
         %{shard: shard} do
      # The other side of #12: requiring BOTH must not turn into requiring neither.
      put_heartbeat(@peer, Storage.now_ms() - Storage.steal_margin_ms() - 60_000)
      put_raw_lock(shard, @peer, 4, Storage.now_ms() - Storage.steal_margin_ms() - 60_000)

      assert {:ok, %{took_over: true, epoch: 5}} =
               Storage.acquire_lease(shard, "me@node#1", 30_000),
             "a dead owner's shard must still be recoverable, and the epoch must advance"
    end

    test "no heartbeat object at all falls back to the lock TTL (the legacy path proper)",
         %{shard: shard} do
      # No heartbeat file is written: this is a node that runs no heartbeat server at all, which
      # is exactly this suite's own configuration.
      put_raw_lock(shard, @peer, 2, Storage.now_ms() + 60_000)
      assert {:error, {:held, @peer, _}} = Storage.acquire_lease(shard, "me@node#1", 30_000)

      put_raw_lock(shard, @peer, 2, Storage.now_ms() - Storage.steal_margin_ms() - 60_000)
      assert {:ok, %{took_over: true}} = Storage.acquire_lease(shard, "me@node#1", 30_000)
    end
  end

  describe "#9 — a drained coordinator frees the lock, it does not just stop" do
    test "serve, drain, and the lock is FREE for a peer to take", %{shard: shard} do
      capture_log(fn ->
        {:ok, conn} = ShardExecutor.open(shard)
        {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))
        :ok = ShardExecutor.close(conn)

        assert {:held, _owner} = Storage.lease_holder(shard)
        assert :ok = Shards.drain(shard, 5_000)

        # Stopping is not enough. A coordinator that stops WITHOUT releasing leaves the shard
        # locked with nobody running it — un-openable on this node or any other until the TTL
        # expires, which is the "permanently stranded" half of #9.
        assert :free = Storage.lease_holder(shard),
               "the coordinator stopped without releasing its lock — the shard is stranded"

        # And the release is real, not just a probe artifact: a peer can acquire cleanly.
        assert {:ok, lease} = Storage.acquire_lease(shard, @peer, 30_000)
        assert :ok = Storage.release_lease(shard, lease)
      end)
    end

    test "the writes survived the drain — releasing is not skipping the flush", %{shard: shard} do
      capture_log(fn ->
        {:ok, conn} = ShardExecutor.open(shard)
        {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('durable')"))
        :ok = ShardExecutor.close(conn)
        :ok = Shards.drain(shard, 5_000)

        # Re-open: cold, from the stored object. If the drain released without flushing, this
        # comes back empty — the failure mode a lock-only assertion cannot see.
        {:ok, conn2} = ShardExecutor.open(shard)

        assert {:ok, %{rows: [["durable"]]}} =
                 ShardExecutor.execute(conn2, stmt("SELECT v FROM kv"))

        :ok = ShardExecutor.close(conn2)
      end)
    end
  end
end
