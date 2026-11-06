defmodule Fathom.Test.Env do
  @moduledoc """
  `Application.put_env` for tests, restored when the test exits.

  A bare `Application.put_env/3` in a test leaks into every later test on the node unless the test
  remembers to undo it. `put!/3` records the previous value (or its absence) and registers an
  `on_exit` that puts it back or deletes the key. Call it from the test process or a `setup`.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  @doc "Set `key` on `:fathom` for the rest of this test."
  @spec put!(atom(), term()) :: :ok
  def put!(key, value), do: put!(:fathom, key, value)

  @doc "Set `key` on `app` for the rest of this test."
  @spec put!(atom(), atom(), term()) :: :ok
  def put!(app, key, value) do
    previous = Application.fetch_env(app, key)

    on_exit(fn ->
      case previous do
        {:ok, old} -> Application.put_env(app, key, old)
        :error -> Application.delete_env(app, key)
      end
    end)

    Application.put_env(app, key, value)
  end
end
