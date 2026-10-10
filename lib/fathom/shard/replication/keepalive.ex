defmodule Fathom.Shard.Replication.Keepalive do
  @moduledoc """
  TCP keepalive options for the replication sockets (expert review 2026-10-10 #11).

  The follower's reader does `:gen_tcp.recv(sock, 0)` — no timeout, by design: an idle connection is
  a healthy one. But a hard peer loss (power, kernel panic, a partition that drops rather than
  resets) leaves that connection half-open forever: the reader, its up-to-8 workers and their fds
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

    if idle > 0 do
      [{:keepalive, true} | tuned(:os.type(), idle, interval, count)]
    else
      [{:keepalive, true}]
    end
  end

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
