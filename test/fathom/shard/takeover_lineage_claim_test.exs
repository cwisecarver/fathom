defmodule Fathom.Shard.TakeoverLineageClaimTest do
  @moduledoc """
  A takeover claims a lineage strictly above anything the previous holder could have shipped, and
  records it in the lock (expert review 2026-09-29 #3; decided 2026-09-29).

  ## The defect

  The open took `next_lineage(object_head)`. An owner X that claimed L+1, shipped WAL frames to its
  followers and crashed before flushing left the object at L, so its successor Y took L+1 too and
  restarted the WAL ordinal under a lineage the followers already held from X — two histories with
  one label, which `Promote.fresher?/2` and `FollowerLog.decide/2` cannot tell apart.

  ## What is pinned

    * a steal of a claim-less (fresh-created) lock claims `next_lineage + 1`, and the claim is in
      the lock body on disk;
    * a SECOND crash in a row goes above the first stealer's recorded claim — where "+1 on steal"
      alone would collide again;
    * a same-owner reclaim of a stale lock claims too (a previous coordinator of this node);
    * a renew keeps the recorded claim;
    * with replication off nothing is claimed and the lock body is unchanged;
    * end to end, a coordinator that took over opens at exactly the recorded claim.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Local

  setup do
    prev = Application.get_env(:fathom, :replication_enabled)
    Application.put_env(:fathom, :replication_enabled, true)
    id = "claim_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_enabled),
        else: Application.put_env(:fathom, :replication_enabled, prev)

      for dir <- [Local.dir(), Fathom.Shard.data_dir()],
          p <- Path.wildcard(Path.join(dir, "#{id}*")),
          do: File.rm_rf(p)
    end)

    %{id: id}
  end

  defp lock_path(id), do: Path.join(Local.dir(), "#{id}.lock")
  defp lock_body(id), do: id |> lock_path() |> File.read!() |> Jason.decode!()

  # An object whose stored lineage is `lineage`.
  defp stored_at!(id, lineage) do
    src = Path.join(System.tmp_dir!(), "#{id}_src.db")
    File.write!(src, "bytes")

    expected =
      case Local.object_etag(id) do
        {:ok, etag} -> etag
        _ -> nil
      end

    {:ok, _etag, _} = Local.flush(id, src, expected, nil, lineage)
    File.rm(src)
  end

  # The holder "crashes": its lock stays, with no heartbeat and a TTL long past.
  defp crash!(id) do
    body = lock_body(id) |> Map.put("expires_at_ms", System.system_time(:millisecond) - 600_000)
    File.write!(lock_path(id), Jason.encode!(body))
  end

  test "a crash-steal chain claims strictly above every previous holder, and records it", %{
    id: id
  } do
    stored_at!(id, 4)

    # X: a fresh create. It uses next_lineage(object) = 5 and records nothing.
    {:ok, x} = Storage.acquire_lease(id, "x@node", 60_000)
    refute Map.has_key?(x, :claim)
    refute Map.has_key?(lock_body(id), "claim")
    crash!(id)

    # Y steals. X may have shipped at 5, so Y goes to 6 — pre-fix Y reopened at 5.
    {:ok, y} = Storage.acquire_lease(id, "y@node", 60_000)
    assert y.took_over
    assert y.claim == 6

    assert lock_body(id)["claim"] == 6,
           "the claim must be durable in the lock for the next stealer"

    crash!(id)

    # Z steals Y's lock. The object never moved (nobody flushed), so next_lineage is still 5 —
    # "+1 on steal" would say 6 again and collide with Y. The recorded claim says go above 6.
    {:ok, z} = Storage.acquire_lease(id, "z@node", 60_000)
    assert z.claim == 7
    assert lock_body(id)["claim"] == 7
  end

  test "a claim recorded below the object's next lineage still yields a strictly higher one", %{
    id: id
  } do
    stored_at!(id, 4)
    {:ok, _} = Storage.acquire_lease(id, "x@node", 60_000)
    crash!(id)
    {:ok, %{claim: 6}} = Storage.acquire_lease(id, "y@node", 60_000)

    # Y flushed at its claim before dying, so the object now says 6 → next_lineage 7.
    stored_at!(id, 6)
    crash!(id)
    assert {:ok, %{claim: 7}} = Storage.acquire_lease(id, "z@node", 60_000)
  end

  test "a same-owner reclaim of a stale lock claims above it too", %{id: id} do
    stored_at!(id, 4)
    {:ok, _} = Storage.acquire_lease(id, "x@node", 60_000)
    crash!(id)

    # The same node, a new coordinator: its predecessor may have shipped at 5.
    assert {:ok, %{claim: 6}} = Storage.acquire_lease(id, "x@node", 60_000)
  end

  test "a renew keeps the recorded claim", %{id: id} do
    stored_at!(id, 4)
    {:ok, _} = Storage.acquire_lease(id, "x@node", 60_000)
    crash!(id)
    {:ok, y} = Storage.acquire_lease(id, "y@node", 60_000)

    {:ok, _} = Local.renew_lease(id, y, 60_000)
    assert lock_body(id)["claim"] == 6
  end

  test "with replication OFF nothing is claimed and the lock body is unchanged", %{id: id} do
    Application.put_env(:fathom, :replication_enabled, false)
    stored_at!(id, 4)
    {:ok, _} = Storage.acquire_lease(id, "x@node", 60_000)
    crash!(id)

    {:ok, y} = Storage.acquire_lease(id, "y@node", 60_000)
    refute Map.has_key?(y, :claim)
    assert Map.keys(lock_body(id)) |> Enum.sort() == ["epoch", "expires_at_ms", "owner"]
  end

  test "a coordinator that took over opens at exactly the recorded claim", %{id: id} do
    stored_at!(id, 4)
    File.mkdir_p!(Local.dir())

    File.write!(
      lock_path(id),
      Jason.encode!(%{
        "owner" => "dead@elsewhere",
        "epoch" => 3,
        "expires_at_ms" => System.system_time(:millisecond) - 600_000
      })
    )

    {:ok, pid} = Fathom.Shards.ensure(id)
    on_exit(fn -> Fathom.Shards.drain(id, 2_000) end)

    # lineage/1 is a call, so it also waits out the open (handle_continue) before the lock is read.
    assert Fathom.Shard.lineage(pid) == 6, "the open must use the claim its lock records"
    assert lock_body(id)["claim"] == 6
  end
end
