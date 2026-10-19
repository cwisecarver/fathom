defmodule Fathom.HranaWsHibernateTest do
  @moduledoc """
  Expert review 2026-10-19 #6: the WebSocket connection process (django-libsql, the primary prod
  path) is the ThousandIsland handler, started with `thousand_island_options[:genserver_options]`,
  NOT with the Filo.Plug opts that reach `Filo.Stream`. With no `hibernate_after` there it held the
  heap of the largest result it ever built (~951 KiB idle after one 2000-row x 8-col response).

  Boots a real listener with `Fathom.Application.hrana_transport_options/0`, runs one large SELECT
  over a real WebSocket, idles past a short `hibernate_after`, and asserts the connection process
  hibernated and its heap shrank.
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.HranaClient
  alias Fathom.Shard.Connection

  @hibernate_ms 200

  setup do
    prev = Application.get_env(:fathom, :hrana_ws_hibernate_ms)
    Application.put_env(:fathom, :hrana_ws_hibernate_ms, @hibernate_ms)

    shard = "wshib#{System.unique_integer([:positive])}"
    path = Path.join(Fathom.Shard.data_dir(), "#{shard}.db")
    File.mkdir_p!(Fathom.Shard.data_dir())

    {:ok, sup, port} =
      HranaClient.start_listener(
        thousand_island_options: Fathom.Application.hrana_transport_options()
      )

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :hrana_ws_hibernate_ms),
        else: Application.put_env(:fathom, :hrana_ws_hibernate_ms, prev)

      HranaClient.stop_listener(sup)
      _ = Fathom.Shards.drain(shard, 1_000)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s)

      for p <- Path.wildcard(Path.join(Fathom.Shard.Storage.Local.dir(), "#{shard}*")),
          do: File.rm(p)
    end)

    %{shard: shard, path: path, port: port, sup: sup}
  end

  defp seed!(path) do
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "PRAGMA journal_mode=WAL")

    :ok =
      Connection.exec(
        conn,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, f TEXT, g TEXT)"
      )

    for id <- 1..2000 do
      v = String.duplicate("x", 24)

      {:ok, _} =
        Connection.query(conn, "INSERT INTO t VALUES (?,?,?,?,?,?,?,?)", [id, v, v, v, v, v, v, v])
    end

    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    File.write!(path <> ".etag", "-")
    :ok
  end

  defp wait_hibernated(pid, deadline) do
    case :erlang.process_info(pid, :current_function) do
      # OTP 29 parks a hibernated gen_server in gen_server:loop_hibernate/4; older OTPs show
      # erlang:hibernate/3.
      {:current_function, {:gen_server, :loop_hibernate, _}} ->
        true

      {:current_function, {:erlang, :hibernate, _}} ->
        true

      _ ->
        if System.monotonic_time(:millisecond) > deadline do
          false
        else
          receive do
          after
            25 -> wait_hibernated(pid, deadline)
          end
        end
    end
  end

  test "the options carry a hibernate_after for the connection handler" do
    gs = Fathom.Application.hrana_transport_options()[:genserver_options]
    assert gs[:hibernate_after] == @hibernate_ms
    assert gs[:spawn_opt][:fullsweep_after] == 10
  end

  test "an idle WS connection hibernates and drops the heap of a large result", ctx do
    :ok = seed!(ctx.path)

    {:ok, c} = HranaClient.connect(ctx.port, ctx.shard)
    {:ok, c, res} = HranaClient.execute(c, "SELECT * FROM t")
    assert length(res.rows) == 2000

    [{_, bandit, _, _}] =
      Supervisor.which_children(ctx.sup) |> Enum.filter(&(elem(&1, 0) != Filo.Streams))

    [handler] = ThousandIsland.connection_pids(bandit) |> then(fn {:ok, pids} -> pids end)

    {:memory, before} = :erlang.process_info(handler, :memory)

    assert wait_hibernated(handler, System.monotonic_time(:millisecond) + 5_000),
           "the WS connection process never hibernated (no hibernate_after on the handler)"

    {:memory, after_mem} = :erlang.process_info(handler, :memory)

    assert after_mem < before,
           "hibernation must shrink the heap: #{before} -> #{after_mem}"

    # Still serves after waking.
    {:ok, c, res} = HranaClient.execute(c, "SELECT count(*) FROM t")
    assert [[2000]] = res.rows
    :ok = HranaClient.close(c)
  end
end
