defmodule Fathom.Test.IdentityShipper do
  @moduledoc """
  A stand-in shipper that answers a push under a CONFIGURABLE identity.

  Exists to pin one contract, from the primary's side: **`Replication.ship_quorum/3` keys its
  expectation map on the term the caller handed it, and on nothing else.**

  That was not true before expert review 2026-08-20 #24. All three modules normalised a shipper
  reference with `Process.whereis(name) || name` before using it as a key, on the assumption that
  replies always identify themselves by pid. `Process.whereis/1` returns `nil` for a shipper
  mid-restart under the `DynamicSupervisor` or a node mid-`Membership` swap — so the expectation
  was filed under the ATOM while the ack came back under the PID, a good ack was scored
  `:offset_mismatch`, and with N=3/Q=2 two of those drive `Quorum.settle/1` to `:impossible` and
  fail the tenant's write with `FILO_NO_QUORUM` for no reason at all.

  A real `Shipper` cannot express the mismatch, because it is now the half that was fixed: it
  always answers under its own `id`. So this double answers under whatever identity the test
  gives it, which is the only way to assert that the primary is keying on the caller's term
  rather than quietly re-deriving one.

  Not a `GenServer`: `Shipper.push/2` is a `cast`, and a raw receive loop makes the frame the
  primary actually puts on the wire visible in the test rather than hidden behind a callback.
  """

  alias Fathom.Shard.Replication.Protocol.Push

  @doc """
  Start a stand-in registered as `name`, answering acks under `reply_as`.

  Pass `reply_as: :pid` for the pre-fix behaviour (identify by `self()`), or any term to identify
  as that term. Registered because the point of the exercise is being addressed by name.
  """
  @spec start(atom(), term()) :: pid()
  def start(name, reply_as) do
    pid = spawn(fn -> loop(reply_as) end)
    Process.register(pid, name)
    pid
  end

  defp loop(reply_as) do
    receive do
      {:"$gen_cast", {:push, %Push{} = p, from, _reserved}} ->
        id = if reply_as == :pid, do: self(), else: reply_as
        send(from, {:repl_reply, id, {:ack, p.shard_id, p.offset + byte_size(p.payload)}})
        loop(reply_as)

      _ ->
        loop(reply_as)
    end
  end
end
