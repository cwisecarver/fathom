defmodule Fathom.Shard.Storage.FlushFenceTest do
  # Finding #15: the fenced flush/3 puts the etag on the data write (If-Match, or If-None-Match:*
  # for a brand-new shard), so a stale PUT can't clobber a stealer. These pin the storage-level
  # primitives: Local's content-hash fence semantics + etag-returning pull/object_etag, and that
  # the S3 backend actually issues the conditional header.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.{Local, S3}

  describe "Local flush/3, pull, object_etag" do
    setup do
      dir =
        Path.join(System.tmp_dir!(), "fathom_flushfence_#{System.unique_integer([:positive])}")

      File.mkdir_p!(dir)
      prev = Application.get_env(:fathom, Local)
      Application.put_env(:fathom, Local, dir: dir)

      on_exit(fn ->
        File.rm_rf!(dir)

        if prev,
          do: Application.put_env(:fathom, Local, prev),
          else: Application.delete_env(:fathom, Local)
      end)

      %{
        dir: dir,
        shard: "s_#{System.unique_integer([:positive])}",
        local: Path.join(dir, "local.db")
      }
    end

    test "pull returns the object etag; object_etag matches without transferring", %{
      shard: shard,
      local: local
    } do
      # `{:absent, nil}` — nothing stored, so nothing written (expert review 2026-08-01 #24).
      # `object_etag/1` is a pure query and keeps its `{:ok, nil}`.
      assert {:absent, nil} = Storage.pull(shard, local)
      assert {:ok, nil} = Storage.object_etag(shard)

      File.write!(local, "v1")
      assert :ok = Storage.flush(shard, local)

      assert {:ok, etag} = Storage.object_etag(shard)
      assert is_binary(etag)
      # A fresh pull of the same object reports the same etag.
      assert {:ok, ^etag} = Storage.pull(shard, Path.join(Path.dirname(local), "copy.db"))
    end

    test "flush/3 writes when the expected etag matches and advances the etag", %{
      shard: shard,
      local: local
    } do
      File.write!(local, "v1")
      assert {:ok, e1} = Storage.flush(shard, local, nil)

      File.write!(local, "v2")
      assert {:ok, e2} = Storage.flush(shard, local, e1)
      assert e2 != e1
      assert {:ok, ^e2} = Storage.object_etag(shard)
    end

    test "flush/3 is superseded (no clobber) when the object changed under us", %{
      shard: shard,
      local: local,
      dir: dir
    } do
      File.write!(local, "ours")
      assert {:ok, e1} = Storage.flush(shard, local, nil)

      # A stealer overwrites the object; our next flush still holds the OLD etag.
      File.write!(Path.join(dir, "#{shard}.db"), "stolen")

      File.write!(local, "ours-v2")
      assert {:error, :superseded} = Storage.flush(shard, local, e1)
      assert File.read!(Path.join(dir, "#{shard}.db")) == "stolen"
    end

    test "flush/3 with nil etag is create-only: superseded if the object already exists", %{
      shard: shard,
      local: local,
      dir: dir
    } do
      File.write!(Path.join(dir, "#{shard}.db"), "someone-else")
      File.write!(local, "ours")
      # nil expected = If-None-Match:* — the object exists, so we must not overwrite it.
      assert {:error, :superseded} = Storage.flush(shard, local, nil)
      assert File.read!(Path.join(dir, "#{shard}.db")) == "someone-else"
    end
  end

  describe "S3 flush/3 conditional header" do
    setup do
      test = self()

      plug = fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        send(
          test,
          {:put, Plug.Conn.get_req_header(conn, "if-match"),
           Plug.Conn.get_req_header(conn, "if-none-match"), body}
        )

        conn |> Plug.Conn.put_resp_header("etag", "\"new\"") |> Plug.Conn.send_resp(200, "")
      end

      prev = Application.get_env(:fathom, S3)

      Application.put_env(:fathom, S3,
        bucket: "b",
        region: "us-east-1",
        access_key_id: "k",
        secret_access_key: "s",
        endpoint: "https://s3.example",
        path_style: true,
        req_plug: plug
      )

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, S3, prev),
          else: Application.delete_env(:fathom, S3)
      end)

      tmp = Path.join(System.tmp_dir!(), "s3flush_#{System.unique_integer([:positive])}.db")
      File.write!(tmp, "body")
      on_exit(fn -> File.rm(tmp) end)
      %{tmp: tmp}
    end

    test "a known etag issues If-Match", %{tmp: tmp} do
      assert {:ok, _new} = S3.flush("shard_x", tmp, "\"etag-1\"")
      assert_received {:put, ["\"etag-1\""], [], "body"}
    end

    test "a nil etag issues If-None-Match:* (create-only)", %{tmp: tmp} do
      assert {:ok, _new} = S3.flush("shard_x", tmp, nil)
      assert_received {:put, [], ["*"], "body"}
    end
  end
end
