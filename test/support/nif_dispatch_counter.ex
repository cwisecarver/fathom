defmodule Fathom.Test.NifDispatchCounter do
  @moduledoc """
  Counts the DIRTY-scheduler NIF dispatches `Exqlite.Sqlite3NIF` receives while a function runs
  (perf review 2026-10-14 #7/#21).

  Every dirty NIF call is a scheduler hand-off (~1-2 us of dispatch on an idle node, far more when
  the +SDio pool is busy), so the number of calls per statement is a DETERMINISTIC cost proxy that
  other load on the machine cannot perturb the way a timing can. The set below mirrors the
  `ERL_NIF_DIRTY_JOB_*` entries of exqlite 0.42's `sqlite3_nif.c` table; the cheap non-dirty NIFs
  (`bind_*`, `set_busy_timeout`, `cancel`, `errmsg`) are not counted.

  Uses call tracing on the calling process, so only NIF calls made by the process that runs `fun`
  are counted (that is where `Connection.query/4` and `Connection.open/2` execute).
  """

  @dirty ~w(open close execute changes prepare reset step multi_step columns last_insert_rowid
            transaction_status serialize deserialize release enable_load_extension
            set_update_hook set_authorizer set_log_hook interrupt)a

  @doc """
  Runs `fun` and returns `{result, counts}`, where `counts` is `%{total: n, by_fun: %{name => n}}`.
  """
  @spec measure((-> result)) :: {result, %{total: non_neg_integer(), by_fun: map()}}
        when result: term()
  def measure(fun) do
    # trace_pattern matches only LOADED modules: an unloaded NIF module silently counts nothing.
    {:module, _} = Code.ensure_loaded(Exqlite.Sqlite3NIF)
    me = self()
    tracer = spawn_link(fn -> collect(me, []) end)

    for f <- @dirty do
      :erlang.trace_pattern({Exqlite.Sqlite3NIF, f, :_}, true, [:local])
    end

    :erlang.trace(me, true, [:call, {:tracer, tracer}])

    result =
      try do
        fun.()
      after
        :erlang.trace(me, false, [:call])
        :erlang.trace_pattern({Exqlite.Sqlite3NIF, :_, :_}, false, [:local])
      end

    send(tracer, :stop)

    calls =
      receive do
        {:calls, calls} -> calls
      after
        5_000 -> raise "NifDispatchCounter tracer did not answer"
      end

    by_fun = Enum.frequencies(calls)
    {result, %{total: length(calls), by_fun: by_fun}}
  end

  defp collect(parent, acc) do
    receive do
      {:trace, _pid, :call, {Exqlite.Sqlite3NIF, f, _args}} -> collect(parent, [f | acc])
      :stop -> send(parent, {:calls, Enum.reverse(acc)})
    end
  end
end
