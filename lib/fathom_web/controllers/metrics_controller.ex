defmodule FathomWeb.MetricsController do
  @moduledoc """
  Prometheus scrape endpoint (`GET /admin/metrics`, behind the admin BasicAuth) — the same
  in-process reporter (`:fathom_metrics`) the dashboard reads, exposed for external
  Prometheus/Grafana. Returns an empty body when the metrics layer is disabled.
  """
  use FathomWeb, :controller

  def index(conn, _params) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, scrape())
  end

  defp scrape do
    :fathom_metrics |> TelemetryMetricsPrometheus.Core.scrape() |> IO.iodata_to_binary()
  rescue
    _ -> ""
  catch
    :exit, _ -> ""
  end
end
