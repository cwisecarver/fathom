defmodule Fathom.Shard.Extension do
  @moduledoc """
  Loads `fathom_udf`, the SQLite loadable extension that supplies the user-defined functions
  Django's SQLite backend registers on its own client connections (expert review 2026-08-01 #19).

  ## Why an extension at all

  Django registers ~35 Python functions on every connection it opens
  (`django/db/backends/sqlite3/_functions.py`). Under `django-libsql` the SQL crosses the wire and
  is compiled by **fathom's** SQLite, where those functions do not exist — so ordinary querysets
  (`__year`, `__date`, `__regex`, `Trunc*`, `F()` arithmetic on a `DurationField`) raise
  `OperationalError` while basic CRUD works.

  The finding recommended registering them "via exqlite's scalar-function registration". exqlite
  0.37.0 has no such API — no `create_function`, no scalar or aggregate registration, in either
  `Exqlite.Sqlite3` or `Exqlite.Sqlite3NIF`. What it *does* expose is `enable_load_extension/2`.

  ## The security shape, which is the whole reason this is a module and not two inline calls

  Extension loading is a privileged capability on a multi-tenant engine: with it enabled, one
  `SELECT load_extension('/path/to/evil.so')` from a tenant is arbitrary code execution inside the
  node. So the sequence is **enable → load ours → disable**, and the disable is not optional or
  best-effort. `load/1` returns an error if it cannot re-disable, and `Fathom.Shard.Connection`
  treats that as a failed open rather than serving a connection with the door left open.

  Verified: after `enable_load_extension(conn, false)`, a tenant's `load_extension(...)` fails with
  `not authorized` (see `test/fathom/shard/extension_test.exs`, which asserts this directly rather
  than trusting the sequence).

  ## Configuration

    * `config :fathom, :sqlite_extension` —
      * unset (default) — load `priv/sqlite_ext/<artifact>` when it exists, skip when it does not.
        A node without the artifact behaves exactly as fathom did before #19.
      * `false` — never load, even if the artifact is present.
      * a path string — load that file instead.

  The default is "load it if it's there" rather than an opt-in flag because this is a
  *compatibility* feature: an operator who has to discover and set a flag to make an unchanged
  Django app work has not really been given the compatibility.
  """

  require Logger

  alias Exqlite.Sqlite3

  # SQLite derives an entry-point symbol from the filename when `load_extension` is given one
  # argument. Passing it explicitly instead means renaming or relocating the artifact cannot
  # silently break loading with an "unable to find entry point" error.
  @entry_point "sqlite3_fathomudf_init"

  @doc """
  Loads the extension into `conn`, leaving extension loading **disabled** afterwards.

  Returns `:ok` when the extension was loaded, `:skipped` when there is nothing to load (no
  artifact, or disabled by config), or `{:error, reason}`.
  """
  @spec load(reference()) :: :ok | :skipped | {:error, term()}
  def load(conn) do
    case path() do
      nil -> :skipped
      path -> do_load(conn, path)
    end
  end

  defp do_load(conn, path) do
    with :ok <- Sqlite3.enable_load_extension(conn, true),
         :ok <- load_statement(conn, path) do
      disable(conn)
    else
      {:error, reason} ->
        # Re-disable on the failure path too. An extension that failed to load must not leave the
        # capability enabled behind it — that is the one outcome strictly worse than not having the
        # functions at all.
        _ = disable(conn)
        {:error, reason}
    end
  end

  # A failure to re-disable is fatal for this connection: it would be served to a tenant with
  # arbitrary-extension-loading available. There is no safe way to continue, so the open fails.
  defp disable(conn) do
    case Sqlite3.enable_load_extension(conn, false) do
      :ok -> :ok
      {:error, reason} -> {:error, {:could_not_disable_extension_loading, reason}}
    end
  end

  # The path is BOUND, not interpolated. `load_extension` is an ordinary SQL function, so it takes
  # a parameter like any other — and AGENTS.md's "never interpolate values into SQL" applies to a
  # filesystem path with quotes in it exactly as it applies to tenant data. `Sqlite3.execute/2`
  # takes no bindings, hence prepare/bind/step.
  defp load_statement(conn, path) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, "SELECT load_extension(?1, ?2)") do
      try do
        with :ok <- Sqlite3.bind(stmt, [path, @entry_point]),
             {:row, _} <- Sqlite3.step(conn, stmt) do
          :ok
        else
          {:error, reason} -> {:error, reason}
          other -> {:error, {:unexpected_load_result, other}}
        end
      after
        Sqlite3.release(conn, stmt)
      end
    end
  end

  # Cache key for the resolved DEFAULT path. Only the default branch is cached, because only it
  # costs a filesystem stat.
  @default_key {__MODULE__, :default_path}

  @doc """
  The extension path to load, or `nil` when there is nothing to load.

  Runs on **every connection open** — one per Hrana stream — so the default branch caches its
  `File.exists?` result in `:persistent_term`. Measured 2026-08-05: that stat was ~21 µs of the
  ~78 µs this adds to a stream open, i.e. more than a quarter of the cost of the whole feature,
  paid to re-answer a question whose answer cannot change while the node runs. The explicit
  branches (`false`, a configured path) are plain `Application.get_env` reads and are not cached,
  which is what keeps tests that flip the config between cases working.

  Call `refresh/0` if the artifact appears or disappears under a running node.
  """
  @spec path() :: Path.t() | nil
  def path do
    case Application.get_env(:fathom, :sqlite_extension, :default) do
      false ->
        nil

      :default ->
        cached_default()

      path when is_binary(path) ->
        path

      other ->
        Logger.warning(
          "ignoring :sqlite_extension=#{inspect(other)} — expected a path, false, or unset"
        )

        nil
    end
  end

  defp cached_default do
    case :persistent_term.get(@default_key, :miss) do
      :miss ->
        resolved =
          case default_path() do
            p when is_binary(p) -> if File.exists?(p), do: p, else: nil
          end

        # A `:persistent_term.put` triggers a global GC scan, so this must happen a bounded number
        # of times — once per node here, not once per open.
        :persistent_term.put(@default_key, resolved)
        resolved

      resolved ->
        resolved
    end
  end

  @doc """
  Drops the cached default-path resolution.

  For tests that create or remove the artifact under a running node, and for an operator who
  installs it without restarting.
  """
  @spec refresh() :: :ok
  def refresh do
    :persistent_term.erase(@default_key)
    :ok
  end

  @doc """
  Where `mix compile.fathom_udf` installs the artifact.

  Uses `:code.priv_dir/1` so this resolves inside a release, where `priv/` lives under the
  application's lib directory rather than the project root.
  """
  @spec default_path() :: Path.t()
  def default_path do
    case :code.priv_dir(:fathom) do
      {:error, :bad_name} -> Path.join(["priv", "sqlite_ext", artifact_name()])
      dir -> Path.join([dir, "sqlite_ext", artifact_name()])
    end
  end

  @doc """
  Whether Django's UDFs will be available on connections opened from now on.

  Read by `Fathom.Application` at boot to log the compatibility posture once, so an operator
  debugging an `OperationalError: no such function: django_date_extract` can tell from the startup
  log whether the node ever had them.
  """
  @spec available?() :: boolean()
  def available?, do: path() != nil

  defp artifact_name do
    case :os.type() do
      {:unix, :darwin} -> "libfathom_udf.dylib"
      {:win32, _} -> "fathom_udf.dll"
      _ -> "libfathom_udf.so"
    end
  end
end
