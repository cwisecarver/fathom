defmodule Fathom.LoggerOverloadTest do
  @moduledoc """
  Expert review 2026-11-07 #45: a failure storm turned `Logger` into a node-wide stall. OTP's
  `logger_std_h` defaults (`sync_mode_qlen: 10, drop_mode_qlen: 200`) put the handler into SYNCHRONOUS
  mode between 10 and 200 queued lines: every `Logger` call becomes a call into the handler, and
  against a stalled stdout (a Docker `json-file` / journald / log-shipper pipe) the CALLER blocks up
  to the handler's 5 s call timeout. Coordinators, `Heartbeat` and the `Recorder` all log, so one slow
  sink stalls healthy shards. `config/config.exs` now sets `sync_mode_qlen == drop_mode_qlen`: the
  handler goes from async straight to dropping and there is no blocking window.

  Two halves, because a config key that never reaches the handler passes a config-only test:

    * the LIVE default handler (the one the booted app is using) carries the policy;
    * a handler built from exactly those overload keys, writing to a device that never answers,
      does not block 40 concurrent callers, while the same handler with OTP's defaults does.
  """
  use ExUnit.Case, async: false

  # A formatter that costs ~0.1 ms of CPU per event, so the handler's single process falls behind 40
  # producers on any machine (without it the queue only builds when the scheduler happens to
  # interleave the producers, and the control below is timing-dependent).
  defmodule SlowFormatter do
    @moduledoc false
    def format(_event, _config) do
      Enum.reduce(1..5_000, 0, fn i, acc -> :erlang.phash2({i, acc}) end)
      "overload probe\n"
    end
  end

  @overload_keys [
    :sync_mode_qlen,
    :drop_mode_qlen,
    :flush_qlen,
    :burst_limit_enable,
    :burst_limit_max_count,
    :burst_limit_window_time
  ]

  defp live_overload do
    {:ok, %{config: config}} = :logger.get_handler_config(:default)
    Map.take(config, @overload_keys)
  end

  test "the booted default handler never enters a blocking sync mode" do
    cfg = live_overload()

    assert cfg.sync_mode_qlen >= cfg.drop_mode_qlen,
           "sync_mode_qlen #{cfg.sync_mode_qlen} < drop_mode_qlen #{cfg.drop_mode_qlen}: between " <>
             "them every log call blocks its caller on a slow sink (OTP default is 10 / 200)"

    assert cfg.flush_qlen >= cfg.drop_mode_qlen

    # A deliberate burst limit, not the 500/s default that dropped 94% of an 8000-line second.
    assert cfg.burst_limit_max_count >= 2_000
  end

  # 40 processes log 100 lines each into a handler whose sink never answers. Returns how many of the
  # 40 got all their calls back within `wait_ms`, and the slowest single call among those.
  defp storm(overload, wait_ms) do
    sink = spawn(fn -> Process.sleep(:infinity) end)
    id = :"logger_overload_probe_#{System.unique_integer([:positive])}"
    filter_id = :"not_probe_#{System.unique_integer([:positive])}"

    # Only this test's events reach the probe handler, and none of them reach the real stdout.
    only_probe = {fn ev, probe -> if ev.meta[:probe] == probe, do: ev, else: :stop end, id}

    :ok =
      :logger.add_handler(id, :logger_std_h, %{
        config: Map.merge(%{type: {:device, sink}}, overload),
        level: :all,
        filter_default: :log,
        filters: [only_probe: only_probe],
        formatter: {SlowFormatter, %{}}
      })

    :ok =
      :logger.add_handler_filter(
        :default,
        filter_id,
        {fn ev, _ -> if ev.meta[:probe], do: :stop, else: :ignore end, []}
      )

    tasks =
      for _ <- 1..40 do
        Task.async(fn ->
          for _ <- 1..100 do
            started = System.monotonic_time(:millisecond)
            :logger.warning("overload probe", %{probe: id})
            System.monotonic_time(:millisecond) - started
          end
        end)
      end

    results = Task.yield_many(tasks, wait_ms)

    # Cleanup BEFORE asserting: unblock the handler's writer, drop stuck callers, remove both the
    # handler and the filter on the real one.
    for {task, nil} <- results, do: Task.shutdown(task, :brutal_kill)
    Process.exit(sink, :kill)
    :logger.remove_handler_filter(:default, filter_id)
    :logger.remove_handler(id)

    maxes = for {_task, {:ok, durations}} <- results, do: Enum.max(durations)
    {length(maxes), Enum.max([0 | maxes])}
  end

  test "with the app's overload policy a stalled sink blocks no caller" do
    {finished, slowest_ms} = storm(live_overload(), 5_000)

    assert finished == 40, "only #{finished} of 40 callers got their log calls back"
    assert slowest_ms < 1_000, "the slowest log call took #{slowest_ms} ms against a stalled sink"
  end

  # The control: same storm, OTP's defaults. If this stopped blocking, the test above would pass
  # without the config and prove nothing.
  test "the same storm DOES block callers under OTP's default policy" do
    defaults = %{sync_mode_qlen: 10, drop_mode_qlen: 200, flush_qlen: 1000}
    {finished, _slowest_ms} = storm(defaults, 1_500)

    assert finished < 40,
           "all 40 callers finished under the OTP defaults: the fixture no longer reproduces the stall"
  end
end
