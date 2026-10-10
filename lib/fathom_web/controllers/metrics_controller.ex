defmodule FathomWeb.MetricsController do
  @moduledoc """
  Prometheus scrape endpoint (`GET /admin/metrics`, behind the admin BasicAuth) — the same
  in-process reporter (`:fathom_metrics`) the dashboard reads, exposed for external
  Prometheus/Grafana. Returns an empty 200 only when the metrics layer is disabled; when it is
  enabled but the scrape fails (reporter down, ETS gone) it logs and answers 503, so Prometheus
  records a failed scrape (`up == 0`) instead of an empty-but-healthy one (expert review
  2026-10-10 #W7).
  """
  use FathomWeb, :controller

  require Logger

  def index(conn, _params) do
    conn = put_resp_content_type(conn, "text/plain")

    if Fathom.Admin.enabled?() do
      case scrape() do
        {:ok, body} -> send_resp(conn, 200, body)
        :error -> send_resp(conn, 503, "metrics scrape failed")
      end
    else
      send_resp(conn, 200, "")
    end
  end

  defp scrape do
    {:ok, Fathom.Telemetry.scrape()}
  rescue
    e ->
      Logger.error("metrics scrape failed: #{Exception.message(e)}")
      :error
  catch
    :exit, reason ->
      Logger.error("metrics scrape exited: #{inspect(reason)}")
      :error
  end
end
