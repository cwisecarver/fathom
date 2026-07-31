defmodule Fathom.WireBlobTest do
  @moduledoc """
  BLOB values over the real Hrana wire.

  Nothing covered this before. Every wire benchmark is TPC-B or TPC-C — INTEGER, REAL and TEXT
  only — and the fidelity tests around `Fathom.Keystone` stop at the file (copy, flush, pull).
  So blobs had never made a round trip through `Filo.Value` encoding, WebSocket framing and back,
  and two things were waiting there.
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.HranaClient
  alias Fathom.Shard.Connection

  setup do
    shard = "wireblob#{System.unique_integer([:positive])}"
    path = Path.join(Fathom.Shard.data_dir(), "#{shard}.db")
    File.mkdir_p!(Fathom.Shard.data_dir())

    {:ok, sup, port} = HranaClient.start_listener()

    on_exit(fn ->
      HranaClient.stop_listener(sup)
      _ = Fathom.Shards.drain(shard, 1_000)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s)

      for p <- Path.wildcard(Path.join(Fathom.Shard.Storage.Local.dir(), "#{shard}*")),
          do: File.rm(p)
    end)

    %{shard: shard, path: path, port: port}
  end

  defp seed!(path, rows) do
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "PRAGMA journal_mode=WAL")
    :ok = Connection.exec(conn, "CREATE TABLE b (id INTEGER PRIMARY KEY, v BLOB)")

    for {id, bytes} <- rows do
      {:ok, _} = Connection.query(conn, "INSERT INTO b VALUES (?, ?)", [id, {:blob, bytes}])
    end

    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok
  end

  test "a blob survives a round trip through the wire", %{shard: shard, path: path, port: port} do
    # Deliberately not a multiple of 3 bytes: Hrana base64 is UNPADDED, and the client used to
    # decode with `Base.decode64!/1` — the PADDED decoder — which raised
    # `ArgumentError: incorrect padding` on exactly these lengths. It also had no `{:blob, _}`
    # encode clause at all, so a blob argument raised `FunctionClauseError`. Both survived
    # because no wire benchmark had ever put a blob on the wire.
    bytes = <<0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0xFF>>
    :ok = seed!(path, [{1, bytes}])

    {:ok, c} = HranaClient.connect(port, shard)
    {:ok, c, res} = HranaClient.execute(c, "SELECT v FROM b WHERE id = 1")

    assert [[{:blob, ^bytes}]] = res.rows

    # And a blob must survive as a bound ARGUMENT, not only as a result.
    round = <<1, 2, 3, 250, 251, 252, 0>>
    {:ok, c, _} = HranaClient.execute(c, "INSERT INTO b VALUES (?, ?)", [2, {:blob, round}])
    {:ok, c, back} = HranaClient.execute(c, "SELECT v FROM b WHERE id = 2")
    assert [[{:blob, ^round}]] = back.rows

    :ok = HranaClient.close(c)
  end

  # CHARACTERIZATION, not approval. The storage class does not survive the wire, and the cause is
  # below fathom: `deps/exqlite/c_src/sqlite3_nif.c` reads `sqlite3_column_type` and then returns
  # `make_binary` for BOTH `SQLITE_BLOB` and `SQLITE_TEXT`, so the class is discarded in the NIF
  # before Elixir sees the value. `Filo.Value.encode_json/1` cannot recover it and guesses from
  # UTF-8 validity: valid ⇒ text, invalid ⇒ blob.
  #
  # The guess is right for ordinary data and wrong at both edges, and it is DATA-DEPENDENT — the
  # same column returns `blob` or `text` depending on the bytes in the row. `sqld` does not have
  # this problem; it reads the real column type.
  #
  # Measured exposure (200k samples per shape): pickle, gzip and PNG payloads are classified
  # correctly 100% of the time — their magic bytes are invalid UTF-8 START bytes — as is anything
  # past ~16 bytes of entropy. The failure is confined to SHORT blobs of all-low bytes, where it
  # is total. Documented for operators, with the pad/prefix workaround, in `docs/data-path.md`
  # ("Known limitation"). The only complete fix is upstream: an opt-in exqlite option returning
  # `{:blob, bytes}` for SQLITE_BLOB.
  test "KNOWN GAP: a blob of valid-UTF-8 bytes comes back typed as text",
       %{shard: shard, path: path, port: port} do
    :ok = seed!(path, [{1, "text-as-blob"}, {2, <<0xFF, 0xFE, 0xFD>>}])

    {:ok, c} = HranaClient.connect(port, shard)
    {:ok, c, res} = HranaClient.execute(c, "SELECT v FROM b ORDER BY id")
    :ok = HranaClient.close(c)

    # SQLite holds both as class `blob`.
    {:ok, conn} = Connection.open(path)
    {:ok, %{rows: classes}} = Connection.query(conn, "SELECT typeof(v) FROM b ORDER BY id", [])
    Connection.close(conn)
    assert classes == [["blob"], ["blob"]]

    # The wire disagrees on the first one: valid UTF-8 bytes are emitted as text, so the client
    # gets a String where it stored bytes. The second is not valid UTF-8, so the guess lands on
    # blob and happens to be right.
    assert [[wrong], [right]] = res.rows
    assert wrong == "text-as-blob", "blob with UTF-8 bytes is currently returned as text"
    assert right == {:blob, <<0xFF, 0xFE, 0xFD>>}
  end
end
