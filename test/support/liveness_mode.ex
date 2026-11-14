defmodule Fathom.Test.LivenessMode do
  @moduledoc """
  Put a test in one of the coordinator's two liveness modes, and prove it took.

  A coordinator's `acquire_gen` is fixed at open: non-nil means HEARTBEAT mode (the node heartbeat
  proves liveness), nil means LEGACY mode (per-shard renew PUTs). They have different fence, renewal
  and release paths, and `config/test.exs` defaults to legacy (`heartbeat_server: false`) while
  production runs heartbeat, so anything touching lease/fence/flush/drop must be run in both
  (AGENTS.md § Testing).

      for mode <- [:legacy, :heartbeat] do
        test "does the thing (\#{mode})" do
          LivenessMode.set!(unquote(mode))
          {:ok, pid} = ... open the coordinator ...
          LivenessMode.assert!(pid, unquote(mode))
        end
      end

  `set!/2` MUST run before the coordinator opens (the mode is fixed at open). `assert!/2` exists
  because a scenario that silently ran legacy twice looks like two-mode coverage and is one.
  Replaces `set_mode!/1` and `assert_mode!/2` copied into ~67 test files with three argument orders
  (cleanliness audit 2026-11-13 #37).
  """

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [start_supervised!: 1]

  alias Fathom.Shard
  alias Fathom.Shard.Heartbeat

  @type mode :: :legacy | :heartbeat

  @doc """
  `:legacy`: assert the heartbeat is OFF (the test env default). `:heartbeat`: start a supervised
  `Heartbeat` (long TTL so it never lapses on its own; a test that wants a lapse forces one) and
  wait for it to come up. `opts` go to `Heartbeat` (e.g. `owner:`, or `ttl_ms:` to override the
  30 s default); they are ignored for `:legacy`.
  """
  @spec set!(mode(), keyword()) :: :ok
  def set!(mode, opts \\ [])

  def set!(:legacy, _opts) do
    refute Heartbeat.running?(),
           "legacy mode needs the heartbeat OFF; config/test.exs default is heartbeat_server: false"

    :ok
  end

  def set!(:heartbeat, opts) do
    hb = start_supervised!({Heartbeat, Keyword.merge([ttl_ms: 30_000], opts)})
    # A synchronous call: the heartbeat has finished init, so `running?/0` below is not racing it.
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  @doc """
  Assert the coordinator `pid` really opened in `mode`: non-nil `acquire_gen` is heartbeat mode,
  nil is legacy.
  """
  @spec assert!(pid(), mode()) :: :ok
  def assert!(pid, :heartbeat) when is_pid(pid) do
    assert is_integer(Shard.acquire_gen(pid)),
           "expected heartbeat mode (non-nil acquire_gen): the mode setup did not take"

    :ok
  end

  def assert!(pid, :legacy) when is_pid(pid) do
    assert Shard.acquire_gen(pid) == nil,
           "expected legacy mode (nil acquire_gen): a heartbeat was running when the coordinator opened"

    :ok
  end
end
