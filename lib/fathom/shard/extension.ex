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

  ## Since the auto-extension (expert review 2026-10-22 r8 #14)

  `Fathom.Shard.Connection` no longer calls `load/1` when `open_mode/0` answers `:auto`: the
  functions arrive with `sqlite3_open` itself, so a `Connection.open` handle never has extension
  loading enabled at all, not even for the length of a load. The capability is now turned on only
  on the private control connection (`control/2`), which loads a separate entry point, runs one
  fathom-authored statement and is closed. `native/fathom_udf/src/auto.rs` has the full argument;
  `load/1` itself, and its enable -> load -> disable contract, are unchanged for the callers that
  still use it (an artifact without the auto entry point, the A2 Session's registry connection).

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
    case open_mode() do
      :skipped -> :skipped
      :auto -> attach_auto(conn)
      _per_open_or_error -> do_load(conn, path())
    end
  end

  # UNDER THE AUTO-EXTENSION (expert review 2026-10-22 r8 #14) a connection opened after arming
  # already carries every function, and an explicit load would FAIL on it: SQLite refuses to
  # replace a function while a statement is active ("unable to delete/modify user-function due to
  # active statements"), and the `SELECT load_extension(...)` doing the replacing is one. Found by
  # `replication_unshipped_checkpoint_test.exs`: the A2 Session's registry connection lost its
  # registration, so the hook checkpointed a replicated WAL. So install what an explicit load would
  # have added on top of the functions, the WAL hook and the backstop's handlers, with
  # `fathom_conn_init()`: one dispatch, no `dlopen`, and loading never enabled. A connection opened
  # BEFORE the arming has no such function, and takes the explicit load as before.
  defp attach_auto(conn) do
    case Sqlite3.execute(conn, "SELECT fathom_conn_init()") do
      :ok -> :ok
      {:error, _no_such_function} -> do_load(conn, path())
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
  defp load_statement(conn, path, entry \\ @entry_point) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, "SELECT load_extension(?1, ?2)") do
      try do
        with :ok <- Sqlite3.bind(stmt, [path, entry]),
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

  # --- the process-global auto-extension (expert review 2026-10-22 r8 #14, Q1=A) ----------------
  #
  # `load/1` costs five dirty dispatches and a `dlopen`/`dlclose` per connection. Once ARMED, the
  # extension's `auto_init` is registered with `sqlite3_auto_extension`, so SQLite registers the
  # functions inside every `sqlite3_open` instead and `Fathom.Shard.Connection` skips `load/1`.
  # native/fathom_udf/src/auto.rs carries the whole security argument; in short, no
  # `Connection.open` handle ever has extension loading enabled any more, and the only connection
  # that does is the private CONTROL connection below.

  @armed_key {__MODULE__, :armed}
  @control_entry "sqlite3_fathomudf_control_init"

  @doc """
  How `Fathom.Shard.Connection.open/2` gets the functions onto the handle it is ABOUT to open.
  Call it before `sqlite3_open`: an auto-extension applies to connections opened after it is
  registered.

    * `:auto` — the auto-extension is armed for the configured artifact; the functions are already
      there after `sqlite3_open`, and the caller installs the hook and handlers itself with
      `SELECT fathom_conn_init()`.
    * `:per_open` — the artifact predates the auto-extension: call `load/1` per connection, as before.
    * `:skipped` — nothing to load (no artifact, or `:sqlite_extension` is `false`).
    * `{:error, reason}` — the configured artifact cannot be loaded; the open must fail, exactly as
      a failed `load/1` failed it.

  The steady state is one `Application.get_env` and two `:persistent_term` reads. Arming, and
  disarming after a config change to `false`, run once per change.
  """
  @spec open_mode() :: :auto | :per_open | :skipped | {:error, term()}
  def open_mode do
    armed = :persistent_term.get(@armed_key, nil)

    case path() do
      nil ->
        if armed, do: disarm(armed)
        :skipped

      path ->
        case armed do
          {^path, mode} -> mode
          other -> arm(path, other)
        end
    end
  end

  @doc "The artifact the auto-extension is armed for, or nil."
  @spec armed_path() :: Path.t() | nil
  def armed_path do
    case :persistent_term.get(@armed_key, nil) do
      {path, :auto} -> path
      _ -> nil
    end
  end

  defp arm(path, previous) do
    if previous, do: disarm(previous)

    case control(path, &exec_row(&1, "SELECT fathom_auto_extension(1)", [])) do
      {:ok, 1} ->
        put_armed({path, :auto})
        :auto

      {:error, reason} ->
        # The control entry point is missing from an artifact built before r8 #14, but the
        # per-connection one may still load: serve it the old way rather than fail every open.
        if loads_per_connection?(path) do
          Logger.warning(
            "fathom_udf at #{path} has no auto-extension entry point (#{inspect(reason)}); " <>
              "loading it per connection. Rebuild the artifact (mix compile.fathom_udf)."
          )

          put_armed({path, :per_open})
          :per_open
        else
          {:error, reason}
        end
    end
  end

  defp disarm({path, :auto}) do
    _ = control(path, &exec_row(&1, "SELECT fathom_auto_extension(0)", []))
    :persistent_term.erase(@armed_key)
    :ok
  end

  defp disarm(_per_open) do
    :persistent_term.erase(@armed_key)
    :ok
  end

  # A `:persistent_term.put` triggers a global GC scan: bounded by config changes, never per open.
  defp put_armed(value), do: :persistent_term.put(@armed_key, value)

  defp loads_per_connection?(path) do
    with {:ok, conn} <- Sqlite3.open(":memory:") do
      try do
        do_load(conn, path) == :ok
      after
        Sqlite3.close(conn)
      end
    end
  end

  @doc false
  # Runs `fun.(conn)` on a private CONTROL connection: a fresh `:memory:` handle loaded with the
  # artifact's control entry point, which registers the node-global control functions on it and
  # nowhere else. Extension loading is enabled for the load only and disabled before `fun` runs, so
  # even this handle never executes anything with the capability on; it is closed on the way out
  # whatever happens. Never hand this handle to anyone.
  @spec control(Path.t(), (reference() -> {:ok, term()} | {:error, term()})) ::
          {:ok, term()} | {:error, term()}
  def control(path, fun) do
    with {:ok, conn} <- Sqlite3.open(":memory:") do
      try do
        with :ok <- Sqlite3.enable_load_extension(conn, true),
             loaded <- load_statement(conn, path, @control_entry),
             :ok <- disable(conn),
             :ok <- loaded do
          fun.(conn)
        else
          {:error, reason} ->
            _ = disable(conn)
            {:error, reason}
        end
      after
        Sqlite3.close(conn)
      end
    end
  end

  @doc false
  # One bound statement on a control connection, answering its single value.
  @spec exec_row(reference(), String.t(), list()) :: {:ok, term()} | {:error, term()}
  def exec_row(conn, sql, args) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, sql) do
      try do
        with :ok <- Sqlite3.bind(stmt, args),
             {:row, [value]} <- Sqlite3.step(conn, stmt) do
          {:ok, value}
        else
          {:error, reason} -> {:error, reason}
          other -> {:error, {:unexpected_result, other}}
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

  Read by `Fathom.Application.check_udf_extension!/0` at boot (expert review 2026-10-10 #8): a prod
  node without the artifact refuses to start unless `ALLOW_NO_UDF=true` acknowledges the
  downgrade (no engine PRAGMA authorizer, no deadline backstop, no size limits, no Django UDFs).
  Before that nothing called this, and a node built without cargo degraded silently.
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
