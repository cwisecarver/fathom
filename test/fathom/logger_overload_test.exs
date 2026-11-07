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
      does not block a caller, while the same handler with OTP's defaults does.
  """
  use ExUnit.Case, async: false

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

  # The probe, built so the outcome is decided by `logger_olp`'s own rules and not by scheduling.
  #
  # `logger_olp` re-reads the queue length only when it HANDLES an event (`check_load`), and the mode
  # it picks then is what every later caller sees. A dead sink blocks the olp inside that first
  # event's write, so the mode is frozen at whatever the first event found. The probe therefore:
  #
  #   1. adds a handler whose device is a process that never answers,
  #   2. SUSPENDS the olp and lets `queued` callers cast into its mailbox (async mode: no blocking),
  #   3. resumes it; the first event it handles sees `queued` waiting and sets the mode from the
  #      overload keys, then blocks in the write to the sink (the sink tells us when it arrives),
  #   4. sends ONE more log call and reports whether it came back within `wait_ms`.
  #
  # Returns `:returned` (async or drop: the caller is never made to wait) or `:blocked` (sync mode:
  # the caller is parked in a call to an olp that is stuck on the sink).
  defp probe(overload, queued, wait_ms) do
    test_pid = self()

    sink =
      spawn(fn ->
        receive do
          {:io_request, _from, _ref, _req} = req -> send(test_pid, {:sink_got, req})
        end

        Process.sleep(:infinity)
      end)

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
        formatter: {:logger_formatter, %{template: [:msg, "\n"]}}
      })

    :ok =
      :logger.add_handler_filter(
        :default,
        filter_id,
        {fn ev, _ -> if ev.meta[:probe], do: :stop, else: :ignore end, []}
      )

    # `logger_h_common` registers the handler's olp as `<module>_<id>`.
    olp_pid =
      Process.whereis(:"logger_std_h_#{id}") || flunk("the probe handler's olp is not registered")

    try do
      :ok = :sys.suspend(olp_pid)
      for _ <- 1..queued, do: :logger.warning("overload probe", %{probe: id})
      {:message_queue_len, ^queued} = Process.info(olp_pid, :message_queue_len)
      :ok = :sys.resume(olp_pid)

      assert_receive {:sink_got, _}, 5_000

      task = Task.async(fn -> :logger.warning("overload probe", %{probe: id}) end)

      case Task.yield(task, wait_ms) do
        {:ok, :ok} -> :returned
        nil -> :blocked
      end
    after
      Process.exit(sink, :kill)
      :logger.remove_handler_filter(:default, filter_id)
      :logger.remove_handler(id)
    end
  end

  test "with the app's overload policy a stalled sink blocks no caller" do
    assert probe(live_overload(), 50, 1_000) == :returned,
           "a log call blocked behind a stalled sink with #{inspect(live_overload())}"
  end

  # The control: same probe, OTP's defaults. 50 queued lines is between sync_mode_qlen (10) and
  # drop_mode_qlen (200), so the olp goes synchronous and the next caller parks. If this stopped
  # blocking, the test above would pass without the config and prove nothing.
  test "the same probe DOES block a caller under OTP's default policy" do
    defaults = %{sync_mode_qlen: 10, drop_mode_qlen: 200, flush_qlen: 1000}

    assert probe(defaults, 50, 500) == :blocked,
           "the caller was not blocked under the OTP defaults: the fixture no longer reproduces the stall"
  end
end
