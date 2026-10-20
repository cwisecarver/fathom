defmodule Fathom.Shard.Replication.FollowerLabelTest do
  @moduledoc """
  Expert review 2026-10-20 #26: `chaos.sh repl-cpu` attributed reductions by `initial_call` module,
  and follower per-shard workers / push tasks are `:erlang.spawn_opt(fn -> ... end)` processes whose
  initial_call is `{:erlang, :apply, 2}`, so their CPU fell into no role at all. They now carry
  `Process.set_label/1`, which the rig script reads first. This pins the worker label; if it is
  dropped the rig report silently loses the follower's biggest role again.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Replication.Follower

  test "a follower worker labels itself and its initial_call is the anonymous-fun one" do
    {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
    {:ok, port} = :inet.port(l)
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
    {:ok, server} = :gen_tcp.accept(l)

    worker = Follower.spawn_worker(server, :no_such_follower, nil, self(), :atomics.new(1, []))
    Process.unlink(worker)

    on_exit(fn ->
      Process.exit(worker, :kill)
      :gen_tcp.close(client)
      :gen_tcp.close(l)
    end)

    # The precondition that motivated the label: module-based attribution cannot see this process.
    assert {:initial_call, {:erlang, :apply, 2}} = Process.info(worker, :initial_call)

    assert wait_label(worker, 10_000) == :replication_follower_worker
  end

  defp wait_label(_pid, 0), do: :undefined

  defp wait_label(pid, n) do
    case Process.info(pid, :label) do
      {:label, :undefined} ->
        :erlang.yield()
        wait_label(pid, n - 1)

      {:label, label} ->
        label
    end
  end
end
