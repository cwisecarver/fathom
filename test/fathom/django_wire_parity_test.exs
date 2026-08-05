defmodule Fathom.DjangoWireParityTest do
  @moduledoc """
  The last hop: UDF results as they cross the **Hrana wire**, which is what `django-libsql` actually
  receives.

  Every other parity suite stops at `Fathom.Shard.Connection`. That leaves one place a type can
  still be lost — Hrana encodes each value with an explicit type tag
  (`{"type":"integer","value":"12"}`), and `django-libsql` converts back to a Python value from that
  tag, not from the SQL. So a UDF that returns INTEGER 12 inside SQLite but goes out as
  `{"type":"text","value":"12"}` reaches Django as the string `"12"`, and
  `Duration(...) * 2 == timedelta(...)` quietly stops being true.

  That failure is invisible to every test that reads through `Connection`, because at that layer the
  value is already correct. Hence this file: it drives `Filo.Plug` exactly as a client does and
  asserts the type TAG.

  The values themselves are covered exhaustively elsewhere (`django_sql_parity_test.exs` and the two
  generated oracles); this asserts only what those structurally cannot see.
  """
  use Fathom.DataCase, async: false

  import Plug.Test
  import Plug.Conn

  alias Fathom.Directory
  alias Fathom.Shards

  @streams :fathom_wire_parity_streams

  setup do
    start_supervised!({Filo.Streams, name: @streams})
    shard = "wirepar#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Shards.drain(shard, 2_000)

      for dir <- [Fathom.Shard.data_dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    opts =
      Filo.Plug.init(
        executor: Fathom.ShardExecutor,
        streams: @streams,
        key: Filo.Baton.new_key(),
        open_arg: &Fathom.ShardExecutor.shard_from_conn/1
      )

    {:ok, _} = Directory.resolve(shard)
    %{opts: opts, shard: shard}
  end

  defp sql(opts, shard, statement) do
    body = %{
      "baton" => nil,
      "requests" => [%{"type" => "execute", "stmt" => %{"sql" => statement}}]
    }

    conn(:post, "http://#{shard}.fathom.test/v3/pipeline", Jason.encode!(body))
    |> put_req_header("content-type", "application/json")
    |> Filo.Plug.call(opts)
  end

  # Asserts the statement succeeded on the wire and returns its rows (possibly none — DDL and DML
  # return an empty set, which is not a failure).
  defp wire_rows(conn) do
    assert conn.status == 200
    [result] = Jason.decode!(conn.resp_body)["results"]
    assert result["type"] == "ok", "the wire returned an error: #{inspect(result)}"
    result["response"]["result"]["rows"]
  end

  # The raw Hrana cells for the first row — type tag intact, deliberately not decoded.
  defp wire_row(conn) do
    [row | _] = wire_rows(conn)
    row
  end

  defp tags(conn), do: Enum.map(wire_row(conn), & &1["type"])

  test "duration arithmetic keeps its SQLite type across the wire", %{opts: opts, shard: shard} do
    # The bug this file exists for. Django only str()s the + and - branches of
    # _sqlite_format_dtdelta; * and / return numbers. If the wire flattened those to text,
    # `WHERE duration * 2 = 6` would stop matching with no error anywhere.
    assert ["text"] = tags(sql(opts, shard, "SELECT django_format_dtdelta('+',1,2)"))
    assert ["text"] = tags(sql(opts, shard, "SELECT django_format_dtdelta('-',4,1)"))
    assert ["integer"] = tags(sql(opts, shard, "SELECT django_format_dtdelta('*',3,4)"))
    assert ["float"] = tags(sql(opts, shard, "SELECT django_format_dtdelta('/',9,2)"))

    # And the values survive with the type.
    assert [%{"type" => "integer", "value" => "12"}] =
             wire_row(sql(opts, shard, "SELECT django_format_dtdelta('*',3,4)"))
  end

  test "extracts arrive as integers, truncations as text", %{opts: opts, shard: shard} do
    assert ["integer"] = tags(sql(opts, shard, "SELECT django_date_extract('year','2026-08-05')"))

    assert ["integer"] =
             tags(
               sql(
                 opts,
                 shard,
                 "SELECT django_datetime_extract('hour','2026-08-05 13:00:00',NULL,NULL)"
               )
             )

    assert ["text"] =
             tags(sql(opts, shard, "SELECT django_date_trunc('month','2026-08-05',NULL,NULL)"))

    assert ["text"] =
             tags(
               sql(
                 opts,
                 shard,
                 "SELECT django_datetime_cast_date('2026-08-05 12:00:00',NULL,NULL)"
               )
             )

    # Microsecond diffs are large integers — they must not come back as floats, which would lose
    # precision on a big duration.
    assert ["integer"] = tags(sql(opts, shard, "SELECT django_time_diff('12:00:00','11:00:00')"))
  end

  test "regexp arrives as an integer boolean, and NULL stays NULL", %{opts: opts, shard: shard} do
    assert [%{"type" => "integer", "value" => "1"}] =
             wire_row(sql(opts, shard, "SELECT 'abc' REGEXP '^a'"))

    assert [%{"type" => "integer", "value" => "0"}] =
             wire_row(sql(opts, shard, "SELECT 'abc' REGEXP '^z'"))

    assert ["null"] = tags(sql(opts, shard, "SELECT NULL REGEXP '^a'"))
  end

  test "the Python-re-only constructs work over the wire, not just in-process", %{
    opts: opts,
    shard: shard
  } do
    # Lookahead, lookbehind and backreferences used to be a hard error here; the trailing-newline
    # `$` case used to be a silently wrong `0`.
    for {sql_text, want} <- [
          {"SELECT 'foobar' REGEXP 'foo(?=bar)'", "1"},
          {"SELECT 'USD100' REGEXP '(?<=USD)\\d+'", "1"},
          {"SELECT 'aabb' REGEXP '(\\w)\\1'", "1"},
          {"SELECT 'a-slug-1' || char(10) REGEXP '^[a-z0-9-]+$'", "1"},
          {"SELECT 'abc' || char(10) REGEXP 'abc\\Z'", "0"}
        ] do
      assert [%{"type" => "integer", "value" => ^want}] = wire_row(sql(opts, shard, sql_text)),
             "wire mismatch for #{sql_text}"
    end
  end

  test "hashes and text functions arrive as text", %{opts: opts, shard: shard} do
    assert [%{"type" => "text", "value" => "9dd4e461268c8034f5c8564e155c67a6"}] =
             wire_row(sql(opts, shard, "SELECT MD5('x')"))

    assert ["text"] = tags(sql(opts, shard, "SELECT LPAD('x',3,'0')"))
    assert ["text"] = tags(sql(opts, shard, "SELECT REVERSE('abc')"))
    assert ["integer"] = tags(sql(opts, shard, "SELECT BITXOR(1,2)"))
  end

  test "aggregates arrive as floats and a degenerate group as NULL", %{opts: opts, shard: shard} do
    wire_rows(sql(opts, shard, "CREATE TABLE t (v INTEGER)"))
    wire_rows(sql(opts, shard, "INSERT INTO t (v) VALUES (1),(3),(NULL)"))

    assert ["float"] = tags(sql(opts, shard, "SELECT STDDEV_POP(v) FROM t"))
    assert ["null"] = tags(sql(opts, shard, "SELECT VAR_SAMP(v) FROM t WHERE v = 1"))
    assert ["integer"] = tags(sql(opts, shard, "SELECT BIT_XOR(v) FROM t"))
  end
end
