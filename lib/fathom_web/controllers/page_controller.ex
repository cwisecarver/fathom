defmodule FathomWeb.PageController do
  use FathomWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
