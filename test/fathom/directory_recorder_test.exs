defmodule Fathom.Directory.RecorderTest do
  # Exercises the app-global recorder + Postgres, so not async (shared sandbox lets
  # the recorder process write through the test's connection).
  use Fathom.DataCase, async: false

  alias Fathom.Directory
  alias Fathom.Directory.Recorder

  setup do
    # Drain any buffer leftover from a prior test so flush counts are deterministic.
    Recorder.flush()
    :ok
  end

  defp uniq, do: "rec_#{System.unique_integer([:positive])}"

  test "coalesces repeated accesses into one row per shard and batch-flushes" do
    a = uniq()
    b = uniq()

    assert :ok = Recorder.record(a)
    assert :ok = Recorder.record(a)
    assert :ok = Recorder.record(b)

    # Two distinct shards, despite three records (a was coalesced).
    assert Recorder.flush() == 2

    assert {:ok, %{shard_id: ^a, schema_version: 0, status: "active"}} = Directory.get(a)
    assert {:ok, %{shard_id: ^b, schema_version: 0, status: "active"}} = Directory.get(b)
  end

  test "flushing an empty buffer is a no-op" do
    assert Recorder.flush() == 0
  end

  test "re-recording bumps recency without resetting version or status" do
    a = uniq()

    assert :ok = Recorder.record(a)
    assert Recorder.flush() == 1
    {:ok, first} = Directory.get(a)

    # Advance the shard's lifecycle the way a migration would.
    {:ok, _} = Directory.cutover(a, 5)

    assert :ok = Recorder.record(a)
    assert Recorder.flush() == 1
    {:ok, second} = Directory.get(a)

    # The on-conflict path only touches recency.
    assert second.schema_version == 5
    assert second.status == "active"
    assert DateTime.compare(second.last_active_at, first.last_active_at) in [:gt, :eq]
  end

  @tag :bench
  test "record/1 stays off the Postgres hot path (sub-50µs ETS write)" do
    id = uniq()
    # Warm the path, then measure a steady-state buffer write.
    Recorder.record(id)
    {us, :ok} = :timer.tc(fn -> Recorder.record(id) end)

    # An ETS insert is single-digit µs; a synchronous Postgres upsert (~100µs+,
    # see dir_resolve_p50_us) could never land under this ceiling.
    assert us < 50
  end
end
