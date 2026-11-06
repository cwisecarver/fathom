defmodule Fathom.Shard.Replication.Keepalive do
  @moduledoc """
  TCP keepalive options for the replication sockets (expert review 2026-10-10 #11).

  The follower's reader does `:gen_tcp.recv(sock, 0)` — no timeout, by design: an idle connection is
  a healthy one. But a hard peer loss (power, kernel panic, a partition that drops rather than
  resets) leaves that connection half-open forever: the reader, its workers (32 by default) and their fds
  stay allocated, and a seed that was mid-stream keeps its shard lock until the 180 s age takeover
  (`FILO_NO_QUORUM` for that shard meanwhile). The OS default keepalive idle is two hours, which is
  no protection at all, so both ends enable keepalive with a ~30 s idle:

      idle 30 s, then a probe every 10 s, dead after 3 unanswered probes  =>  ~60 s to notice

  The idle / interval / count knobs are not portable through `:gen_tcp`: OTP's `keepidle` option is
  rejected (`einval`) on macOS, whose idle time is `TCP_KEEPALIVE`, and the option set differs by
  OS. So the tuned values are passed as RAW socket options on the two OSes we run on (Linux, the
  deployed fleet; Darwin, development) and every other OS falls back to plain `keepalive: true`
  with that OS's default timers — still correct, just slow to notice (hours, not a minute).

  Overridable with `:replication_keepalive_idle_s`, `:replication_keepalive_interval_s` and
  `:replication_keepalive_count`; `idle_s: 0` disables the tuned values (plain keepalive only).

  ## Unacknowledged data: a user timeout, not keepalive

  Keepalive only probes an IDLE connection. A shipper with a push in flight is not idle: its frame
  sits unacknowledged in the send queue, and the kernel retransmits it on an exponential backoff.
  A follower partitioned (or power-lost) for ~30 s therefore stays "connected" for up to another
  ~20 s after it is back, until the next retransmit lands, and the shard's single per-follower
  waiter answers every commit in the meantime with `:already_in_flight`. Tier 0 on 2026-11-06
  (hard1, liveness RED) lost a shard's quorum that way: two of its three followers were stuck in
  flight 21 s after the faults healed. So the tuned set also carries the OS's user timeout
  (`TCP_USER_TIMEOUT` on Linux, `TCP_RXT_CONNDROPTIME` on Darwin): data unacknowledged for longer
  than `:replication_tcp_user_timeout_ms` (default 10 s, `0` disables) aborts the connection, the
  shipper fails its waiters `:disconnected` and redials. The kernel acknowledges data whatever the
  follower's application is doing, so a slow-but-alive follower is not cut; a follower that stops
  reading is already cut sooner, by the 5 s `send_timeout`.
  """

  require Logger

  # IPPROTO_TCP
  @tcp 6

  @doc "The `:gen_tcp` options enabling keepalive, tuned where the OS lets us."
  @spec opts() :: [term()]
  def opts do
    idle = Application.get_env(:fathom, :replication_keepalive_idle_s, 30)
    interval = Application.get_env(:fathom, :replication_keepalive_interval_s, 10)
    count = Application.get_env(:fathom, :replication_keepalive_count, 3)
    os = :os.type()

    if idle > 0 do
      [{:keepalive, true} | tuned(os, idle, interval, count)] ++
        user_timeout(os, user_timeout_ms())
    else
      [{:keepalive, true}] ++ user_timeout(os, user_timeout_ms())
    end
  end

  @doc false
  @spec user_timeout_ms() :: non_neg_integer()
  def user_timeout_ms, do: Application.get_env(:fathom, :replication_tcp_user_timeout_ms, 10_000)

  @doc """
  Run `fun` (a `:gen_tcp.listen/2` or `connect` closure) with `base` plus keepalive options,
  stepping down if the OS rejects them (`{:error, :einval}`): tuned raw options, then plain
  `keepalive: true`, then none. A keepalive tuning problem must never keep the replication listener
  from starting or a shipper from dialling — it only costs how fast a dead peer is noticed.
  """
  @spec with_fallback([term()], ([term()] -> {:ok, term()} | {:error, term()})) ::
          {:ok, term()} | {:error, term()}
  def with_fallback(base, fun) do
    Enum.reduce_while([opts(), [{:keepalive, true}], []], {:error, :einval}, fn extra, _acc ->
      case fun.(base ++ extra) do
        {:error, :einval} = err ->
          Logger.warning("replication: socket options #{inspect(extra)} rejected (einval)")
          {:cont, err}

        other ->
          {:halt, other}
      end
    end)
  end

  @doc false
  # {level, option number} for each timer, per OS. `nil` = no tuned values on this OS.
  @spec option_numbers(tuple()) :: %{idle: integer(), interval: integer(), count: integer()} | nil
  def option_numbers({:unix, :linux}), do: %{idle: 4, interval: 5, count: 6}
  # TCP_KEEPALIVE (idle), TCP_KEEPINTVL, TCP_KEEPCNT in <netinet/tcp.h>.
  def option_numbers({:unix, :darwin}), do: %{idle: 0x10, interval: 0x101, count: 0x102}
  def option_numbers(_other), do: nil

  @doc false
  # {option number, unit} of the OS's user timeout. Linux TCP_USER_TIMEOUT takes milliseconds;
  # Darwin TCP_RXT_CONNDROPTIME takes seconds. `nil` = not tuned on this OS.
  @spec user_timeout_option(tuple()) :: {integer(), :ms | :s} | nil
  def user_timeout_option({:unix, :linux}), do: {18, :ms}
  def user_timeout_option({:unix, :darwin}), do: {0x80, :s}
  def user_timeout_option(_other), do: nil

  defp user_timeout(_os, ms) when ms <= 0, do: []

  defp user_timeout(os, ms) do
    case user_timeout_option(os) do
      nil -> []
      {opt, :ms} -> [{:raw, @tcp, opt, <<ms::native-32>>}]
      {opt, :s} -> [{:raw, @tcp, opt, <<max(1, div(ms + 999, 1000))::native-32>>}]
    end
  end

  defp tuned(os, idle, interval, count) do
    case option_numbers(os) do
      nil ->
        []

      nums ->
        [
          {:raw, @tcp, nums.idle, <<idle::native-32>>},
          {:raw, @tcp, nums.interval, <<interval::native-32>>},
          {:raw, @tcp, nums.count, <<count::native-32>>}
        ]
    end
  end
end
