defmodule Fathom.Directory.RecorderHotspotTest do
  @moduledoc """
  In-process probe for expert review 2026-10-21 #19: rows the Recorder ships to Postgres per busy
  HTTP-path shard per second, versus rows the directory actually writes.

  The HTTP Hrana path (the SDK shape) checks a shard out once per stream, so a busy shard touches
  the Recorder continuously from one coordinator. Before #19 the 1 s coalescing window was the only
  client-side filter, so each such shard shipped ~1 row/s while the 30 s DB write guard rejected
  ~29 of every 30. This drives N busy shards for `PROBE_S` seconds of real time with a synchronous
  `Recorder.flush/0` per second, and counts, from the repo's query telemetry, the ids each touch
  UPDATE receives and the rows it and the first-sight INSERT write.

  `@moduletag :bench` (excluded by default). Run:

      PROBE_N=1000 PROBE_S=60 mix test test/fathom/directory_recorder_hotspot_test.exs --include bench

  It asserts an order-of-magnitude ceiling only (shipped rows per busy shard per second at most
  2.5x the guard's 1/granularity), which fails against the pre-#19 Recorder (~1 row/s) at the
  default 30 s granularity once the run spans more than one window.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Directory.Recorder

  @moduletag :bench
  @moduletag timeout: 600_000

  setup do
    Recorder.flush()
    test_pid = self()
    handler = "recorder-hotspot-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler,
        [:fathom, :repo, :query],
        fn _event, _m, meta, _ ->
          query = meta[:query] || ""

          if meta[:source] == "shards" do
            cond do
              # The touch UPDATE's first list parameter is the unnest'd shard ids (the SET's
              # `updated_at` comes before it in the parameter list).
              String.starts_with?(query, "UPDATE") and
                  Enum.any?(meta[:params] || [], &is_list/1) ->
                ids = Enum.find(meta[:params], &is_list/1)
                send(test_pid, {:shipped, length(ids), num_rows(meta[:result])})

              String.starts_with?(query, "INSERT") ->
                send(test_pid, {:inserted, num_rows(meta[:result])})

              true ->
                :ok
            end
          end
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    :ok
  end

  defp num_rows({:ok, %{num_rows: n}}), do: n
  defp num_rows(_), do: 0

  defp env_int(name, default), do: String.to_integer(System.get_env(name) || "#{default}")

  test "busy HTTP-path shards ship about one row per granularity window, not one per second" do
    n = env_int("PROBE_N", 200)
    secs = env_int("PROBE_S", 65)
    granularity = Application.get_env(:fathom, :directory_touch_granularity_s, 30)

    shards =
      for i <- 1..n do
        coordinator = spawn(fn -> receive do: (:stop -> :ok) end)
        {"hot_#{System.unique_integer([:positive])}_#{i}", coordinator}
      end

    on_exit(fn -> Enum.each(shards, fn {_, p} -> send(p, :stop) end) end)

    deadline = System.monotonic_time(:millisecond) + secs * 1_000

    drive = fn drive ->
      if System.monotonic_time(:millisecond) < deadline do
        # Ten checkouts per shard per second, then the Recorder's 1 s tick.
        for _ <- 1..10 do
          Enum.each(shards, fn {id, p} -> :ok = Recorder.record(id, p) end)
          Process.sleep(100)
        end

        Recorder.flush()
        drive.(drive)
      end
    end

    drive.(drive)
    {shipped, updated, inserted} = drain_counts({0, 0, 0})

    per_shard_s = shipped / (n * secs)
    written = updated + inserted

    IO.puts(
      "recorder hotspot: N=#{n} #{secs}s granularity=#{granularity}s shipped=#{shipped} " <>
        "(#{Float.round(shipped / secs, 1)} rows/s, #{Float.round(per_shard_s, 4)} rows/busy-shard/s) " <>
        "written=#{written} (updated=#{updated} inserted=#{inserted}) " <>
        "shipped/written=#{Float.round(shipped / max(written, 1), 2)}"
    )

    assert per_shard_s <= 2.5 / granularity,
           "each busy shard shipped #{per_shard_s} rows/s; the DB guard writes at most one per " <>
             "#{granularity} s, so the Recorder is shipping rows Postgres throws away"
  end

  defp drain_counts({s, u, i} = acc) do
    receive do
      {:shipped, ids, rows} -> drain_counts({s + ids, u + rows, i})
      {:inserted, rows} -> drain_counts({s, u, i + rows})
    after
      0 -> acc
    end
  end
end
