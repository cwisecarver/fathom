defmodule Fathom.Snapshots.Retention do
  @moduledoc """
  Grandfather-father-son expiry for scheduled snapshots (expert review 2026-08-01 #18).

  Deliberately a **pure function** over `{id, timestamp}` pairs. The classification is the part
  with the bugs in it — bucket boundaries, "keep the newest per period", what happens at a DST
  change or across a year end — and none of that needs an object store to test. `RetentionJob` is
  then a thin shell that lists, calls `plan/3`, and deletes.

  ## The safety property that matters most

  **Only snapshots this scheduler created are ever eligible for deletion.** `ScheduleJob` labels
  every snapshot it takes `-auto`, and `plan/3` refuses to consider anything else — an operator's
  manual `Snapshots.create("acme", label: "pre-migration")` is invisible to retention and lives
  until someone drops it by hand.

  That asymmetry is on purpose. A manual snapshot is the one an operator took deliberately, usually
  right before something risky; expiring it on a schedule would delete exactly the backup someone
  reached for on purpose. Automatic creation and automatic deletion have to be the same set, or the
  policy is silently destroying data it did not create.

  ## The policy

  `%{hourly: h, daily: d, weekly: w}` keeps the **newest snapshot within each of** the last `h`
  distinct hours, `d` distinct days, and `w` distinct ISO weeks that contain one. A snapshot kept
  by any bucket is kept. Periods are counted by how many *populated* periods exist, not by wall
  clock, so a fleet that was down for a week does not lose its history to the gap.
  """

  @typedoc "A snapshot id paired with the UTC timestamp parsed out of it."
  @type dated :: {String.t(), DateTime.t()}

  @typedoc "How many snapshots to keep per granularity."
  @type policy :: %{
          optional(:hourly) => non_neg_integer(),
          optional(:daily) => non_neg_integer(),
          optional(:weekly) => non_neg_integer()
        }

  # `ScheduleJob`'s reserved label. `Fathom.Snapshots.new_snapshot_id/1` builds
  # `<YYYYMMDDTHHMMSSZ>-<uniq>-<label>`, so an automatic snapshot ends in exactly this.
  @auto_label "auto"

  @doc "The label `ScheduleJob` stamps and `plan/3` requires."
  def auto_label, do: @auto_label

  @doc """
  Splits `ids` into `%{keep: [...], drop: [...], ineligible: [...]}` under `policy`.

  `ineligible` is everything retention refuses to touch — manual snapshots, foreign labels, and
  ids whose timestamp cannot be parsed. Returned rather than silently filtered so the job can log
  what it declined to consider; an id it cannot parse is a signal, not noise.

  `now` is passed in rather than read so the classification is deterministic under test.
  """
  @spec plan([String.t()], policy(), DateTime.t()) :: %{
          keep: [String.t()],
          drop: [String.t()],
          ineligible: [String.t()]
        }
  def plan(ids, policy, now \\ DateTime.utc_now()) do
    {eligible, ineligible} =
      Enum.split_with(ids, fn id -> auto?(id) and parse_timestamp(id) != :error end)

    dated =
      eligible
      |> Enum.map(fn id -> {id, parse_timestamp(id)} end)
      |> Enum.sort_by(fn {_id, ts} -> DateTime.to_unix(ts, :microsecond) end, :desc)

    keep =
      [
        {:hourly, &hour_key/1},
        {:daily, &day_key/1},
        {:weekly, &week_key/1}
      ]
      |> Enum.flat_map(fn {granularity, key_fun} ->
        keep_newest_per_period(dated, key_fun, Map.get(policy, granularity, 0))
      end)
      |> MapSet.new()

    # A snapshot NEWER than `now` cannot be classified into a completed period and would otherwise
    # be dropped by every bucket. Clock skew between nodes makes that reachable, and deleting the
    # most recent backup because a peer's clock ran fast is the worst possible failure here.
    future =
      for {id, ts} <- dated, DateTime.compare(ts, now) == :gt, into: MapSet.new(), do: id

    keep = MapSet.union(keep, future)

    {keep_ids, drop_ids} = Enum.split_with(dated, fn {id, _} -> MapSet.member?(keep, id) end)

    %{
      keep: Enum.map(keep_ids, &elem(&1, 0)),
      drop: Enum.map(drop_ids, &elem(&1, 0)),
      ineligible: ineligible
    }
  end

  # Keep the newest snapshot in each of the `count` most recent POPULATED periods.
  #
  # Counting populated periods rather than stepping back `count` periods from `now` is what makes a
  # gap survivable: after a week of downtime, "the last 24 hourly periods" by wall clock contains
  # nothing, and a naive implementation would keep zero hourly snapshots and fall through to
  # deleting history it was supposed to protect.
  defp keep_newest_per_period(_dated, _key_fun, 0), do: []

  defp keep_newest_per_period(dated, key_fun, count) do
    dated
    # `dated` is newest-first, so the first id seen for a period key IS that period's newest.
    |> Enum.reduce({[], MapSet.new()}, fn {id, ts}, {acc, seen} ->
      key = key_fun.(ts)

      if MapSet.member?(seen, key) do
        {acc, seen}
      else
        {[{key, id} | acc], MapSet.put(seen, key)}
      end
    end)
    |> elem(0)
    # The reduce built it oldest-first; take the newest `count` periods.
    |> Enum.reverse()
    |> Enum.take(count)
    |> Enum.map(&elem(&1, 1))
  end

  defp hour_key(%DateTime{} = ts), do: {ts.year, ts.month, ts.day, ts.hour}
  defp day_key(%DateTime{} = ts), do: {ts.year, ts.month, ts.day}

  # `:calendar.iso_week_number/1` takes an Erlang `{y, m, d}` tuple, not a `%Date{}`.
  #
  # ISO week YEAR, not calendar year: 2027-01-01 is a Friday in ISO week 53 of ISO year 2026, so
  # bucketing on `ts.year` would split one week across two keys and keep an extra snapshot at every
  # year boundary that straddles one.
  defp week_key(%DateTime{} = ts) do
    ts |> DateTime.to_date() |> Date.to_erl() |> :calendar.iso_week_number()
  end

  @doc """
  Whether `id` was created by `ScheduleJob` — i.e. ends in the reserved `-auto` label.

  A manual snapshot with no label, or any other label, is not automatic and retention will not
  consider it.
  """
  @spec auto?(String.t()) :: boolean()
  def auto?(id) when is_binary(id), do: String.ends_with?(id, "-" <> @auto_label)
  def auto?(_), do: false

  @doc """
  Parses the UTC timestamp out of a snapshot id, or `:error`.

  Ids are `<YYYYMMDDTHHMMSSZ>-<uniq>[-<label>]` (`Fathom.Snapshots.new_snapshot_id/1`). Only the
  fixed 16-character prefix is read, so a label containing digits or dashes cannot confuse it.
  """
  @spec parse_timestamp(String.t()) :: DateTime.t() | :error
  def parse_timestamp(id) when is_binary(id) do
    with <<date::binary-size(8), "T", time::binary-size(6), "Z", _rest::binary>> <- id,
         <<y::binary-size(4), mo::binary-size(2), d::binary-size(2)>> <- date,
         <<h::binary-size(2), mi::binary-size(2), s::binary-size(2)>> <- time,
         {:ok, date} <- Date.new(int(y), int(mo), int(d)),
         {:ok, time} <- Time.new(int(h), int(mi), int(s)),
         {:ok, dt} <- DateTime.new(date, time, "Etc/UTC") do
      dt
    else
      _ -> :error
    end
  end

  def parse_timestamp(_), do: :error

  defp int(s) do
    case Integer.parse(s) do
      {n, ""} -> n
      _ -> -1
    end
  end
end
