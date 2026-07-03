defmodule Fathom.Shard.Fence do
  @moduledoc """
  The flush fence: the single decision *"may this coordinator still write this
  shard's data back to storage?"*

  Extracted from `Fathom.Shard` so the most safety-critical logic in the system
  (double-write avoidance across nodes) is unit-testable without the coordinator's
  timers, process lifecycle, or real storage. `check/2` is pure given its injected
  dependencies.

  A coordinator may only flush while it still owns the shard. `check/1` decides:

    * **Legacy mode** (`acquire_gen == nil`, heartbeat off/down): renew the per-shard
      lease (a PUT). `:superseded` means we lost it.
    * **Heartbeat mode** (`acquire_gen` set): the node heartbeat proves liveness.
      `valid_for_write?/1` returns `:ok` (valid with margin AND no lapse since acquire
      ⇒ we still own it and it won't expire mid-write ⇒ write with no per-shard I/O),
      `:revalidate` (a lapse happened ⇒ a steal may have occurred ⇒ re-check the lock
      with a read-only `check_lease` and self-fence if superseded), or `:not_valid`
      (not comfortably valid ⇒ don't write). If the heartbeat process is gone the check
      exits and we fall back to the legacy renew fence.

  Returns `{:ok, updates}` (proceed; `updates` = the `lease`/`acquire_gen` to merge back,
  either possibly refreshed), `:superseded` (self-fence, do NOT flush), or `:skip`
  (ownership unconfirmed; don't write, retry later).
  """
  alias Fathom.Shard.{Heartbeat, Storage}

  @type ctx :: %{
          id: String.t(),
          lease: Storage.lease(),
          ttl_ms: pos_integer(),
          acquire_gen: non_neg_integer() | nil
        }

  @type updates :: %{lease: Storage.lease(), acquire_gen: non_neg_integer() | nil}
  @type result :: {:ok, updates()} | :superseded | :skip

  @doc """
  Decide whether a flush may proceed for `ctx` (`%{id, lease, ttl_ms, acquire_gen}`).
  See the moduledoc for the return contract. `deps` is injected for tests; production
  callers omit it and get the live `Heartbeat` / `Storage`.
  """
  @spec check(ctx()) :: result()
  @spec check(ctx(), map()) :: result()
  def check(ctx, deps \\ default_deps())

  def check(%{acquire_gen: nil} = ctx, deps), do: legacy(ctx, deps)

  def check(%{acquire_gen: gen} = ctx, deps) do
    case heartbeat_valid(gen, deps) do
      :ok -> {:ok, %{lease: ctx.lease, acquire_gen: ctx.acquire_gen}}
      :revalidate -> revalidate(ctx, deps)
      :not_valid -> :skip
      :legacy -> legacy(ctx, deps)
    end
  end

  @doc """
  This node's current heartbeat generation, or `nil` if the heartbeat process is down.
  The coordinator records this at acquire; the flush fence compares against it.
  """
  @spec generation() :: non_neg_integer() | nil
  @spec generation(map()) :: non_neg_integer() | nil
  def generation(deps \\ default_deps()) do
    deps.generation.()
  catch
    # Heartbeat process gone — no generation to record (coordinator uses legacy mode).
    :exit, _ -> nil
  end

  defp heartbeat_valid(gen, deps) do
    deps.valid_for_write.(gen)
  catch
    # Heartbeat process gone — degrade to the per-shard renew fence.
    :exit, _ -> :legacy
  end

  # A lapse happened since acquire; a steal may have occurred during the gap. Confirm
  # ownership with a read-only lock check and refresh the generation so subsequent
  # flushes don't revalidate again.
  defp revalidate(ctx, deps) do
    # Sample the generation BEFORE the ownership re-check, not after: a steal landing between
    # check_lease returning :ok and the generation read would otherwise be folded into the new
    # baseline and hidden, so every later flush would pass the fence unconditionally (finding
    # #5). Capturing it first means any lapse after this point re-trips :revalidate.
    gen = generation(deps)

    case deps.check_lease.(ctx.id, ctx.lease) do
      :ok -> {:ok, %{lease: ctx.lease, acquire_gen: gen}}
      {:error, :superseded} -> :superseded
      {:error, _reason} -> :skip
    end
  end

  defp legacy(ctx, deps) do
    case deps.renew_lease.(ctx.id, ctx.lease, ctx.ttl_ms) do
      {:ok, lease} -> {:ok, %{lease: lease, acquire_gen: ctx.acquire_gen}}
      {:error, :superseded} -> :superseded
      {:error, _reason} -> :skip
    end
  end

  # Live wiring. Rebuilt per call (flushes are infrequent, so the map alloc is noise);
  # tests pass their own fakes instead.
  defp default_deps do
    %{
      valid_for_write: &Heartbeat.valid_for_write?/1,
      generation: &Heartbeat.generation/0,
      check_lease: &Storage.check_lease/2,
      renew_lease: &Storage.renew_lease/3
    }
  end
end
