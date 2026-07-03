defmodule Fathom.Repo do
  use Ecto.Repo,
    otp_app: :fathom,
    adapter: Ecto.Adapters.Postgres
end
