defmodule Fathom.Shards.NovelLimiter do
  @moduledoc """
  A per-node token bucket rate-limiting **novel-shard creation** — the churn half
  of finding #14. `:max_open_shards` bounds how many shards a node holds open;
  this bounds how FAST unseen ids can create new ones. A spray of novel valid
  `Host` subdomains otherwise mints a coordinator + ~3 fds + local file + S3 lock
  PUT + Postgres row per request all the way to the cap; with the limiter, the
  burst budget grants and the rest are refused with
  `{:error, :novel_shard_rate_limited}` (→ HTTP 429) before any of that work runs.

  A shard is "novel" to `Fathom.Shards` when nothing knows it: no running
  coordinator, no local file, and no directory row (the directory read fails
  OPEN — a Postgres outage must never refuse checkouts). Existing-shard cold
  opens (failover warming, idle re-opens) are never limited.

  Gated by `config :fathom, :novel_shard_rate` (grants/second, **nil = off, the
  default** — nothing on the cold path pays until an operator enables it, e.g.
  `NOVEL_SHARD_RATE=5` in prod) with burst `config :fathom, :novel_shard_burst`
  (default `max(10, 2 × rate)`). Legitimate novel-shard creation is tenant
  signup — low single digits per second fleet-wide — so a small rate with a
  modest burst does not throttle real growth.

  Refills lazily from elapsed monotonic time on each grant request — no timer.
  One GenServer per node is deliberate: the check runs only on the
  registry-miss (cold) path, and under a spray the serialized mailbox IS the
  backpressure.
  """
  use GenServer

  @doc false
  # `:now_fun` (a 0-arity ms clock, default monotonic time) exists for deterministic
  # refill tests — production always uses the default.
  def start_link(opts) do
    {now_fun, opts} = Keyword.pop(opts, :now_fun, &default_now_ms/0)
    GenServer.start_link(__MODULE__, now_fun, Keyword.put_new(opts, :name, __MODULE__))
  end

  @doc """
  Requests a token to create a novel shard: `:ok` grants,
  `{:error, :novel_shard_rate_limited}` refuses (and emits
  `[:fathom, :shards, :novel_rate_limited]` telemetry).
  """
  @spec allow(String.t(), GenServer.server()) :: :ok | {:error, :novel_shard_rate_limited}
  def allow(shard_id, server \\ __MODULE__) do
    case GenServer.call(server, :allow) do
      :ok ->
        :ok

      :refused ->
        :telemetry.execute(
          [:fathom, :shards, :novel_rate_limited],
          %{count: 1},
          %{shard_id: shard_id}
        )

        {:error, :novel_shard_rate_limited}
    end
  end

  @doc false
  # Test hook: refill the bucket to the current burst budget (the bucket is node-global
  # state, so tests reset it rather than inherit each other's drained tokens).
  def reset(server \\ __MODULE__), do: GenServer.call(server, :reset)

  @impl true
  def init(now_fun) do
    {:ok, %{tokens: burst() * 1.0, last_ms: now_fun.(), now_fun: now_fun}}
  end

  @impl true
  def handle_call(:reset, _from, state) do
    {:reply, :ok, %{state | tokens: burst() * 1.0, last_ms: state.now_fun.()}}
  end

  def handle_call(:allow, _from, state) do
    now = state.now_fun.()
    # Lazy refill: elapsed time × rate, capped at the burst budget. Rate/burst are read
    # per call so a runtime retune applies without restarting the limiter.
    tokens = min(burst() * 1.0, state.tokens + (now - state.last_ms) * rate() / 1000)

    if tokens >= 1 do
      {:reply, :ok, %{state | tokens: tokens - 1, last_ms: now}}
    else
      {:reply, :refused, %{state | tokens: tokens, last_ms: now}}
    end
  end

  @doc """
  Whether the novel-shard rate gate is ON — i.e. `:novel_shard_rate` is a POSITIVE number.

  `Fathom.Shards` used to decide this with `!= nil`, which is the bug in expert review 2026-08-24
  #20: `NOVEL_SHARD_RATE=0` is the natural operator spelling of "disabled" (the documentation says
  "unset = off"), `runtime.exs` passes whatever `String.to_integer/1` returns, and `0` is not nil —
  so every novel-shard open called `allow/2`, `rate/0` had no clause for it, and the GenServer died
  with a `CaseClauseError`. `limiter_refused?/1` catches the exit and fails closed, so every novel
  shard 429s; but the plane supervisor restarts the process on each call and runs
  `max_restarts: 30` in `max_seconds: 10`. Thirty-one novel-shard requests in ten seconds —
  precisely the signup spray this limiter exists to absorb — then take the DATA PLANE DOWN,
  turning a silent misconfiguration into a remotely-triggerable node outage.

  Reading `0` as "off" rather than refusing it at boot is deliberate: `0` unambiguously means no
  rate limiting, and a boot refusal turns a benign typo into a failed deploy.
  """
  @spec enabled?() :: boolean()
  def enabled? do
    case Application.get_env(:fathom, :novel_shard_rate) do
      rate when is_number(rate) and rate > 0 -> true
      _ -> false
    end
  end

  # `allow/2` is only called when the gate is ON (see `enabled?/0` and `Fathom.Shards`); a rate
  # that is nil or non-positive here — retuned off mid-flight, or a misconfiguration that reached
  # this far — grants everything via an effectively-infinite refill rather than crashing the
  # limiter. The catch-all is defence, not the path: `enabled?/0` is what keeps a zero rate from
  # calling `allow/2` at all.
  defp rate do
    case Application.get_env(:fathom, :novel_shard_rate) do
      rate when is_number(rate) and rate > 0 -> rate
      _ -> 1_000_000
    end
  end

  defp burst do
    Application.get_env(:fathom, :novel_shard_burst) ||
      case Application.get_env(:fathom, :novel_shard_rate) do
        nil -> 10
        rate -> max(10, round(2 * rate))
      end
  end

  defp default_now_ms, do: System.monotonic_time(:millisecond)
end
