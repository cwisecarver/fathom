defmodule FathomWeb.Api.MigrationController do
  @moduledoc """
  Authenticated JSON convergence surface for the schema-migration engine (expert review #25).

  `count_laggards == 0` is the deploy gate — a Django CI/CD must not ship app code that depends on
  the new HEAD until the fleet has converged — but it was an Elixir-only API a CI pipeline couldn't
  call. This exposes it (plus the quarantine + review-queue signals) as `GET /api/migrations/status`
  on the `:4000` endpoint, behind the same admin BasicAuth as the rest of `/api`.

  A thin wrapper over `Fathom.Migrator.status/0`.
  """
  use FathomWeb, :controller

  # GET /api/migrations/status
  def status(conn, _params), do: json(conn, Fathom.Migrator.status())
end
