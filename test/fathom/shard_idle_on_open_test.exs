defmodule Fathom.ShardIdleOnOpenTest do
  @moduledoc """
  Expert review 2026-08-31 #13: a coordinator that is started but NEVER checked out must idle-stop
  on its own.

  The idle timer used to be armed ONLY when the last connection checked back in, on the assumption
  "a coordinator is always checked out right after it starts." If the caller dies between
  `Shards.ensure/1` returning `{:ok, pid}` and `Shard.checkout/1` (a Filo stream killed on client
  disconnect during a slow cold-open, or an `{:already_started, pid}` race), the coordinator held a
  lease + fds with zero connections, zero timers and no LRU row — permanently pinned, unstealable
  in heartbeat mode, reclaimed only at node shutdown, and eroding `:max_open_shards`. The timer is
  now armed at open too.

  Not async: shards + the Registry are node-global.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shards

  setup do
    prev = Application.get_env(:fathom, :shard_idle_ms)
    # Low idle so the open-armed timer fires quickly. Per AGENTS.md this is the deterministic knob
    # for idle-timing tests, not a Process.sleep.
    Application.put_env(:fathom, :shard_idle_ms, 50)
    id = "idleopen_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :shard_idle_ms, prev),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      Shards.drain(id, 2_000)

      for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{id}*")),
          do: File.rm(path)
    end)

    %{id: id}
  end

  test "a coordinator started but never checked out idle-stops instead of leaking", %{id: id} do
    {:ok, pid} = Shards.ensure(id)
    ref = Process.monitor(pid)

    # The caller "died" right after ensure/1 — no connection is ever checked out. With the timer
    # armed only at release (the pre-fix behaviour), this coordinator would live forever holding
    # its lease + fds. It must idle-stop on the open-armed timer instead.
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal},
                   2_000,
                   "a never-checked-out coordinator did not idle-stop — it leaks a lease + fds " <>
                     "and a :max_open_shards slot until node shutdown"
  end
end
