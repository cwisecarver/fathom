defmodule Fathom.Shard.Replication.Budget do
  @moduledoc """
  How many queued WAL bytes this NODE may hold across all its shippers — A2's memory ceiling.
  See `docs/reviews/a2-shipper-feedback-loop-2026-08-16.md`.

  ## Why bytes, and why per node

  `Shipper`'s `:replication_max_queue` counts MESSAGES, per shipper, and no value of it works. The
  measurement that settles it, taken on one shipper forty seconds apart during the 1024-tenant run:
  queued messages went 8,265 → 8,195 (flat) while the binary it held went 6,893 MB → 15,798 MB
  (doubled). **The count is not what grows.** And the cap is per shipper while a node runs one per
  peer, so the 8,192 default permits `8,192 x ~1 MB x 4 peers ~= 32 GB` before firing — the observed
  run peaked at 6,263 queued, comfortably under, and the node died anyway.

  So the quantity is bytes and the scope is the node. That is this module.

  ## Why the check happens in the CALLER, not the shipper

  `:replication_max_queue` is consulted when a message is DEQUEUED, which is why it is soft: a burst
  outruns it, and one run reached 12,828 against a cap of 8,192. A cast cannot be back-pressured
  from the receiving side. So `Shipper.push/2` reserves here, in the committing process, **before**
  the payload is ever handed to a mailbox — a refusal costs one counter read and never allocates a
  queue slot.

  ## Why a ref per shipper incarnation

  Each `Shipper` creates its own `:counters` ref in `init/1` and publishes it here. A single
  node-wide counter would be simpler and would *drift*: a shipper killed with a full mailbox never
  runs the `release/2` calls its queued messages owed, and those bytes would be subtracted from the
  node's budget forever, until replication refused everything. A fresh ref per incarnation makes
  that unrepresentable — the replacement starts at zero and the stranded counts die with the ref.

  ## The reservation is optimistic, and that is deliberate

  `reserve/2` adds first and undoes on overflow, rather than checking then adding. Checking first
  lets every concurrent committer see a total that excludes all the others and pass together; adding
  first means a racer sees our bytes already counted, so the overshoot is bounded by the callers who
  had already started. It cannot be made strictly atomic anyway — the node total is a SUM across
  refs, and there is no atomic read of several counters. A safety net that is approximately right at
  the boundary and exactly right everywhere else is the correct trade here; the bound that has to be
  precise is `Primary.plan/3`'s cap on a single push, which is pure arithmetic.
  """

  alias Fathom.Shard.Replication.Fleet

  # One counter per ref. `:write_concurrency` because every commit on the node touches this.
  @slot 1

  @doc """
  Publish a fresh counter for `name`. Called by `Shipper.init/1`, once per incarnation.

  A `nil` or unregistered shipper is a no-op and its pushes are simply unbounded — that is the
  test-only shape (`start_supervised!` with no `:name`), and refusing to start over it would be a
  worse trade than leaving the net off for a process nothing in production creates.
  """
  @spec install(atom() | nil) :: :ok
  def install(name) when is_atom(name) and not is_nil(name) do
    :persistent_term.put(key(name), :counters.new(1, [:write_concurrency]))
  end

  def install(_name), do: :ok

  @doc "Drop a departed follower's counter. Best-effort — a fresh `install/1` also supersedes it."
  @spec forget(atom() | nil) :: :ok
  def forget(name) when is_atom(name) and not is_nil(name) do
    _ = :persistent_term.erase(key(name))
    :ok
  end

  def forget(_name), do: :ok

  @doc """
  Claim `bytes` of the node's budget for a push about to be enqueued.

  Returns `{:ok, reserved}` — where `reserved` is 0 when no budget applies — or `:rejected`. The
  caller must hand `reserved` to `release/2` exactly once, which is why it is returned rather than
  recomputed: a shipper that released `byte_size(payload)` for a push that never reserved would
  drive its counter negative and hand the node free budget.
  """
  @spec reserve(atom() | pid(), non_neg_integer()) :: {:ok, non_neg_integer()} | :rejected
  def reserve(shipper, bytes) do
    cap = max_bytes()

    with true <- cap > 0 and bytes > 0,
         ref when ref != nil <- ref(shipper) do
      :counters.add(ref, @slot, bytes)

      if queued() > cap do
        :counters.sub(ref, @slot, bytes)
        :rejected
      else
        {:ok, bytes}
      end
    else
      _ -> {:ok, 0}
    end
  end

  @doc "Give back what `reserve/2` claimed, once the push has left the mailbox."
  @spec release(atom() | pid(), non_neg_integer()) :: :ok
  def release(shipper, bytes) when is_integer(bytes) and bytes > 0 do
    case ref(shipper) do
      nil -> :ok
      ref -> :counters.sub(ref, @slot, bytes)
    end

    :ok
  end

  def release(_shipper, _bytes), do: :ok

  @doc """
  Bytes currently queued across every shipper this node ships through.

  Summed over `Fleet.shippers/0` rather than over every ref ever installed, so a follower removed by
  a membership swap stops counting the moment it stops being shipped to.
  """
  @spec queued() :: non_neg_integer()
  def queued do
    Enum.reduce(Fleet.shippers(), 0, fn name, acc -> acc + queued(name) end)
  end

  @doc "Bytes currently queued for one shipper. 0 when it has no counter."
  @spec queued(atom() | pid()) :: non_neg_integer()
  def queued(shipper) do
    case ref(shipper) do
      # Clamped at 0: a release without its reserve is a bug, but reporting a negative budget would
      # let it manufacture headroom rather than surface.
      ref when ref != nil -> max(:counters.get(ref, @slot), 0)
      _ -> 0
    end
  end

  @doc """
  The node-wide ceiling, in bytes. 0 disables the budget entirely.

  1 GiB by default, and the posture behind that number matters more than the number: this is the
  safety net, not the fix. `Primary.plan/3`'s per-push cap is what stops a follower falling
  permanently behind, so in health this must never bite — a bound that sheds load in a clean range
  is how the message-count cap failed (1,024 turned a clean 256-tenant step from 3,505 txn/s /
  0 errors into 1,580 / 5,333). 1 GiB sits far above any healthy burst seen on the rig and ~45x
  below the 45 GB the runaway reached.
  """
  @spec max_bytes() :: non_neg_integer()
  def max_bytes,
    do: Application.get_env(:fathom, :replication_max_queue_bytes, 1024 * 1024 * 1024)

  # Shippers are addressed by registered name on the commit path (`Fleet.shippers/0` publishes
  # names), so the atom clause is the hot one. The pid clause exists for tests and for any caller
  # holding a pid, and resolves to the same key so a reserve and its release cannot land on
  # different counters.
  defp ref(name) when is_atom(name), do: :persistent_term.get(key(name), nil)

  defp ref(pid) when is_pid(pid) do
    case Process.info(pid, :registered_name) do
      {:registered_name, name} when is_atom(name) and name != nil -> ref(name)
      _ -> nil
    end
  end

  defp ref(_other), do: nil

  defp key(name), do: {__MODULE__, name}
end
