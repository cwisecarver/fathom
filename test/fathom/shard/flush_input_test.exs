defmodule Fathom.Shard.FlushInputTest do
  @moduledoc """
  `FlushInput` is the one declaration of what the snapshot-and-upload chain reads off a coordinator
  (cleanliness audit 2026-11-13 #28). The periodic flush task used to get a hand-picked `Map.take`
  and the drop path the whole state, so an omission failed late and quietly. These pin the contract
  that replaces that: both paths build the same struct, and a missing REQUIRED field is a loud error
  at the build site.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.FlushInput

  defp state(overrides \\ %{}) do
    Map.merge(
      %{
        id: "flush_input_shard",
        path: "/tmp/flush_input_shard.db",
        etag: "etag-1",
        lease: %{owner: "n", epoch: 3},
        lineage: 7,
        acquire_gen: 4,
        wal_salt: 11,
        wal_ordinal: 12,
        carried_lineage: nil,
        touch_pre_etag: nil,
        # Fields the struct must NOT copy: the coordinator state is much wider than the flush needs.
        conns: %{},
        flush_task: nil,
        ttl_ms: 30_000
      },
      overrides
    )
  end

  test "from_state copies exactly the declared fields and nothing else of the coordinator" do
    input = FlushInput.from_state(state(), self())

    assert %FlushInput{
             id: "flush_input_shard",
             path: "/tmp/flush_input_shard.db",
             etag: "etag-1",
             lease: %{owner: "n", epoch: 3},
             lineage: 7,
             acquire_gen: 4,
             wal_salt: 11,
             wal_ordinal: 12,
             coordinator: coordinator
           } = input

    assert coordinator == self()
    refute Map.has_key?(input, :conns)
    refute Map.has_key?(input, :ttl_ms)
  end

  test "the drop path builds it with no coordinator, and an absent put_intents is []" do
    input = FlushInput.from_state(state())

    assert input.coordinator == nil
    assert input.put_intents == []

    # The coordinator only holds the key once an attempt has run; present means carried through.
    assert FlushInput.from_state(state(%{put_intents: ["md5-a"]})).put_intents == ["md5-a"]
  end

  test "a coordinator state missing a REQUIRED field fails at the build site, not inside a flush task" do
    for key <- [:id, :path, :etag, :lease, :lineage, :acquire_gen] do
      assert_raise KeyError, fn -> FlushInput.from_state(Map.delete(state(), key)) end
    end
  end

  test "legacy mode (acquire_gen nil) is representable: nil is a value, not an omission" do
    assert %FlushInput{acquire_gen: nil} = FlushInput.from_state(state(%{acquire_gen: nil}))
  end

  test "with_fence merges the fence's lease and acquire_gen and rejects any other key" do
    input = FlushInput.from_state(state())

    merged = FlushInput.with_fence(input, %{lease: %{owner: "n", epoch: 4}, acquire_gen: 5})
    assert merged.lease == %{owner: "n", epoch: 4}
    assert merged.acquire_gen == 5
    # Everything else is untouched.
    assert merged.etag == input.etag and merged.path == input.path

    assert_raise KeyError, fn ->
      FlushInput.with_fence(input, %{lease: input.lease, acquire_gen: 1, surprise: true})
    end
  end
end
