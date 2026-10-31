defmodule Fathom.Host.Proc do
  @moduledoc """
  Pure parsers for the Linux `/proc` and `/proc/sys` files fathom reads to attribute a host-level
  latency tail (expert review 2026-10-29 #1 and #17). They take file CONTENTS, never paths, so the
  tests need no `/proc`; `Fathom.Admin.Measurements` does the reads and treats an unreadable file
  (non-Linux, no PSI) as "absent".

  ## The dirty-writeback threshold

  Follower WAL appends are not fsynced, so they sit in the page cache until background writeback
  starts. On the chaos rig (94 GB VM, default `vm.dirty_background_ratio=10`, so a 9.4 GB threshold
  that was never reached) dirty pages climbed to ~7.9 GB, were flushed in 30 s / 60 s bursts, and every
  `synchronous=FULL` commit fsync waited behind a burst: p99 598 ms, worst 10 s window 1,191 ms.
  `vm.dirty_background_bytes` = 1 GB removed the bursts (p99 417 ms, worst window 454 ms)
  (`docs/reviews/tpc-fleet-repl-on-2026-10-26.md`).

  **Lower is not better.** The same sweep measured 256 MB at p99 485 ms / 3,404 txn/s and 64 MB at
  p99 762 ms / 2,273 txn/s, both worse than 1 GB. Every row is a single run on one VM, so 1 GB is a
  direction for THAT rig, not a calibrated optimum for any other host.
  """

  # The one measured-good value is 1 GB; nothing between it and the rig's 9.4 GB default was run.
  # 2 GB is twice the measured-good value, so it flags hosts well inside the unmeasured range while
  # leaving a deliberate 1-2 GB setting alone. It is a "worth measuring" line, not a limit.
  @warn_above_bytes 2 * 1024 * 1024 * 1024

  @doc "The effective background threshold above which the boot preflight warns, in bytes."
  @spec warn_above_bytes() :: pos_integer()
  def warn_above_bytes, do: @warn_above_bytes

  @doc """
  A `/proc/sys` integer file (`dirty_background_bytes`, `dirty_background_ratio`), or `nil` when the
  contents are not a single non-negative integer.
  """
  @spec parse_int(binary()) :: non_neg_integer() | nil
  def parse_int(body) when is_binary(body) do
    case Integer.parse(String.trim(body)) do
      {n, ""} when n >= 0 -> n
      _ -> nil
    end
  end

  @doc """
  `/proc/meminfo` as `%{key => value}`. `kB` rows are converted to BYTES; unitless rows
  (`HugePages_Total`) keep their raw count. A line that is not `Key: <int>[ kB]` is skipped.
  """
  @spec parse_meminfo(binary()) :: %{String.t() => non_neg_integer()}
  def parse_meminfo(body) when is_binary(body) do
    ~r/^([\w()]+):\s+(\d+)( kB)?\s*$/m
    |> Regex.scan(body, capture: :all_but_first)
    |> Map.new(fn
      [key, n, " kB"] -> {key, String.to_integer(n) * 1024}
      [key, n] -> {key, String.to_integer(n)}
    end)
  end

  @doc """
  `/proc/pressure/io` (PSI): the `some` and `full` `avg10` figures, in percent of wall time stalled
  over the last 10 s. A kind the file does not carry is absent from the result.
  """
  @spec parse_pressure(binary()) :: %{optional(:some_avg10 | :full_avg10) => float()}
  def parse_pressure(body) when is_binary(body) do
    ~r/^(some|full)\s.*?\bavg10=([\d.]+)/m
    |> Regex.scan(body, capture: :all_but_first)
    |> Enum.flat_map(fn [kind, v] ->
      case Float.parse(v) do
        {f, _} -> [{pressure_key(kind), f}]
        :error -> []
      end
    end)
    |> Map.new()
  end

  defp pressure_key("some"), do: :some_avg10
  defp pressure_key("full"), do: :full_avg10

  @doc """
  The kernel's effective background-writeback threshold, in bytes, or `nil` when it cannot be
  computed from what was readable.

  `dirty_background_bytes` wins when it is non-zero: the kernel treats the bytes/ratio pair as
  mutually exclusive (writing one makes the other read 0), so a non-zero bytes value is the live
  threshold whatever the ratio file says. Otherwise it is `ratio` percent of the
  dirtyable memory, approximated here by `MemAvailable` (`MemTotal` on a kernel without it). The
  kernel's own figure is free plus reclaimable file pages, so this is an approximation that drifts
  as anonymous memory grows after boot. A zero ratio with zero bytes is a zero threshold
  (continuous writeback), not `nil`.
  """
  @spec effective_background_bytes(%{
          optional(:bytes) => non_neg_integer() | nil,
          optional(:ratio) => non_neg_integer() | nil,
          optional(:meminfo) => %{String.t() => non_neg_integer()}
        }) :: non_neg_integer() | nil
  def effective_background_bytes(%{bytes: bytes}) when is_integer(bytes) and bytes > 0, do: bytes

  def effective_background_bytes(%{ratio: ratio} = inputs) when is_integer(ratio) do
    meminfo = Map.get(inputs, :meminfo, %{})

    case Map.get(meminfo, "MemAvailable") || Map.get(meminfo, "MemTotal") do
      mem when is_integer(mem) -> div(mem * ratio, 100)
      nil -> nil
    end
  end

  def effective_background_bytes(_inputs), do: nil
end
