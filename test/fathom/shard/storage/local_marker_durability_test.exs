defmodule Fathom.Shard.Storage.LocalMarkerDurabilityTest do
  @moduledoc """
  Expert review 2026-10-12 #25. `Storage.Local.put_tombstone/1` and `put_token_floor/2` wrote with a
  bare `File.write` (open-truncate-write, no fsync): after a power cut the tombstone can be missing
  and the revocation floor can read back EMPTY, i.e. `nil`, meaning "no floor" — a revoked token
  validating again. Invariant: both go through `Storage.atomic_write/2` (fsync, then rename), so
  they are whole-old or whole-new, and leave no temp behind.

  A test cannot cut power, so the mechanism is pinned with a call trace on `Storage.atomic_write/2`
  for these two paths (the same technique as `provenance_first_write_test`), plus the observable
  outcome: content as written, no temp, and `tombstoned_ids/0` never reports a stray temp as an id.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Local

  setup do
    id = "marker_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({Storage, :atomic_write, 2}, false, [:local])
      File.rm(Path.join([Local.dir(), "tombstones", id]))
      File.rm(Path.join([Local.dir(), "tokenfloors", id]))
    end)

    # trace_pattern is a silent no-op on a module that is not loaded yet.
    Code.ensure_loaded!(Storage)
    :erlang.trace_pattern({Storage, :atomic_write, 2}, true, [:local])
    :erlang.trace(:all, true, [:call, {:tracer, self()}])
    %{id: id}
  end

  # The tracer process is never itself traced, so the writes must run in another process.
  defp in_other_process(fun), do: fun |> Task.async() |> Task.await()

  # Trace messages are delivered asynchronously, so wait for each expected call rather than
  # draining the mailbox once.
  defp assert_atomic_write!(path) do
    assert_receive {:trace, _pid, :call, {Storage, :atomic_write, [^path, _]}}, 1_000
  end

  test "put_tombstone goes through atomic_write and leaves no temp", %{id: id} do
    path = Path.join([Local.dir(), "tombstones", id])
    assert :ok = in_other_process(fn -> Local.put_tombstone(id) end)

    assert_atomic_write!(path)
    assert File.exists?(path)
    assert Path.wildcard(path <> ".*") == []
    assert {:ok, ids} = Local.tombstoned_ids()
    assert id in ids
  end

  test "tombstoned_ids ignores a crash-orphaned atomic_write temp", %{id: id} do
    :ok = Local.put_tombstone(id)
    orphan = Path.join([Local.dir(), "tombstones", "#{id}.tmp.123"])
    File.write!(orphan, "")
    on_exit(fn -> File.rm(orphan) end)

    assert {:ok, ids} = Local.tombstoned_ids()
    assert id in ids
    refute "#{id}.tmp.123" in ids
  end

  test "put_token_floor goes through atomic_write; the floor never reads back empty or lower", %{
    id: id
  } do
    path = Path.join([Local.dir(), "tokenfloors", id])

    assert :ok = in_other_process(fn -> Local.put_token_floor(id, 7) end)
    assert {:ok, 7} = Local.read_token_floor(id)
    assert :ok = in_other_process(fn -> Local.put_token_floor(id, 9) end)
    assert {:ok, 9} = Local.read_token_floor(id)

    assert_atomic_write!(path)
    assert_atomic_write!(path)
    assert Path.wildcard(path <> ".*") == []
  end
end
