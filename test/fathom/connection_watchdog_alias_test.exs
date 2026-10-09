defmodule Fathom.ConnectionWatchdogAliasTest do
  @moduledoc """
  Expert review 2026-10-08 #27: a watchdog timeout that lands AFTER its query returned must not
  reach the query owner's mailbox.

  `with_deadline/3` sweeps stale `{:timed_out, _}` when it arms the NEXT guarded query. When the
  near-miss is the last statement of a request there is no next query, and the owner is a
  `Filo.Stream` GenServer without a catch-all `handle_info` — so the late message crashed the stream.
  The watchdog now answers a per-query process alias that is dropped once the query is judged, and
  the runtime discards any send to a dropped alias.

  Deterministic: the test reads the `{:arm, ref, ms, reply_to}` the watchdog received (by tracing it)
  and replays the watchdog's late `{:timed_out, ref}` itself after the query has returned.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  setup do
    prev = Application.get_env(:fathom, :query_timeout_ms)
    Application.put_env(:fathom, :query_timeout_ms, 5_000)

    path = Path.join(System.tmp_dir!(), "wd_alias_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(path, tenant?: true)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :query_timeout_ms, prev),
        else: Application.delete_env(:fathom, :query_timeout_ms)

      Connection.close(conn)
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    end)

    %{conn: conn}
  end

  test "a timeout sent after the query was judged never reaches the owner", %{conn: conn} do
    {:ok, _} = Connection.query(conn, "SELECT 1", [], deadline: true)
    watchdog = Process.get({Connection, :watchdog, conn})
    assert is_pid(watchdog)

    :erlang.trace(watchdog, true, [:receive])
    {:ok, _} = Connection.query(conn, "SELECT 2", [], deadline: true)
    assert_receive {:trace, ^watchdog, :receive, {:arm, ref, _ms, reply_to}}, 1_000
    :erlang.trace(watchdog, false, [:receive])

    # The watchdog's timer firing an instant too late: the query already returned.
    send(reply_to, {:timed_out, ref})

    refute_receive {:timed_out, ^ref},
                   100,
                   "a late timeout reached the owner's mailbox — a GenServer owner would crash on it"
  end
end
