defmodule Fathom.DataCase do
  @moduledoc """
  This module defines the setup for tests requiring
  access to the application's data layer.

  You may define functions here to be used as helpers in
  your tests.

  Finally, if the test case interacts with the database,
  we enable the SQL sandbox, so changes done to the database
  are reverted at the end of every test. If you are using
  PostgreSQL, you can even run database tests asynchronously
  by setting `use Fathom.DataCase, async: true`, although
  this option is not recommended for other databases.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      alias Fathom.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Fathom.DataCase
    end
  end

  setup tags do
    Fathom.DataCase.setup_sandbox(tags)
    :ok
  end

  @doc """
  Sets up the sandbox based on the test tags.
  """
  def setup_sandbox(tags) do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Fathom.Repo, shared: not tags[:async])
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
  end

  @doc """
  A helper that transforms changeset errors into a map of messages.

      assert {:error, changeset} = Accounts.create_user(%{password: "short"})
      assert "password is too short" in errors_on(changeset).password
      assert %{password: ["password is too short"]} = errors_on(changeset)

  """
  def errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Regex.replace(~r"%{(\w+)}", message, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end

  @doc """
  `Directory.resolve/2` plus a recorded first flush: a row that has really been uploaded.

  The rollout sweep deliberately skips a NEVER-FLUSHED `schema_version: 0` row (expert review
  2026-11-07 #51 — a read-only request mints one and no stored object exists, so a migration for
  it can only cancel `:no_live_object`). A test that models a real cold-tail shard behind HEAD must
  therefore say it flushed; a bare `resolve/2` models the phantom.
  """
  @spec resolve_flushed(String.t(), keyword()) :: {:ok, Fathom.Directory.Shard.t()} | :error
  def resolve_flushed(shard_id, opts \\ []) do
    {:ok, _} = Fathom.Directory.resolve(shard_id, opts)
    1 = Fathom.Directory.record_flush_batch([{shard_id, DateTime.utc_now()}])
    Fathom.Directory.get(shard_id)
  end
end
