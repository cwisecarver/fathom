defmodule Mix.Tasks.Fathom.ShardVersionTest do
  # Expert review 2026-10-08 #22: shard objects are stored compressed by default, so a noncurrent
  # bucket version fetched with `aws s3api get-object --version-id` is not a SQLite file, and the
  # docs could only offer a manual decode + MD5 + quick_check recipe. `mix fathom.shard pull
  # --version-id` routes the version through the SAME decode + plaintext-digest verification as a
  # normal pull. Invariants: the request carries the versionId (query-encoded, not spliced), the
  # file written is the decoded plaintext, an unverifiable or mismatching version writes nothing,
  # and a non-S3 backend fails clearly.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.S3

  @plain "SQLite format 3\0 pretend this is a database page"
  # Characters real S3 version ids carry (`+`, `/`, `=`) that a hand-spliced query string would
  # mangle — the stub compares the DECODED value, so a wrong encoding is a 404 here.
  @vid "3HL4kqtJlcp+XroD/TmJ="

  setup do
    dir = Path.join(System.tmp_dir!(), "s3ver_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    prev_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    prev_backend = Application.get_env(:fathom, :shard_storage)
    prev_s3 = Application.get_env(:fathom, S3)

    on_exit(fn ->
      Mix.shell(prev_shell)
      restore_env(:shard_storage, prev_backend)
      restore_env(S3, prev_s3)
      File.rm_rf!(dir)
    end)

    %{dir: dir}
  end

  defp restore_env(key, nil), do: Application.delete_env(:fathom, key)
  defp restore_env(key, v), do: Application.put_env(:fathom, key, v)

  defp use_s3(plug) do
    Application.put_env(:fathom, :shard_storage, S3)

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: plug
    )
  end

  defp md5_hex(bin), do: Base.encode16(:crypto.hash(:md5, bin), case: :lower)

  # Serves a zstd body with the enc marker + `md5_meta` digest ONLY for the right versionId; the
  # current object (no versionId) and any other version are 404 — so a pass proves the param went
  # out and decoded to exactly @vid.
  defp version_plug(test_pid, md5_meta) do
    body = :zstd.compress(@plain) |> IO.iodata_to_binary()

    fn conn ->
      conn = Plug.Conn.fetch_query_params(conn)
      send(test_pid, {:get, conn.request_path, conn.query_params})

      if conn.query_params["versionId"] == @vid do
        conn
        |> Plug.Conn.put_resp_header("etag", ~s(") <> md5_hex(body) <> ~s("))
        |> Plug.Conn.put_resp_header("x-amz-meta-fathom-enc", "zstd")
        |> then(fn c ->
          if md5_meta,
            do: Plug.Conn.put_resp_header(c, "x-amz-meta-fathom-md5", md5_meta),
            else: c
        end)
        |> Plug.Conn.send_resp(200, body)
      else
        Plug.Conn.send_resp(conn, 404, "")
      end
    end
  end

  test "pulls the named version, decoded and verified, to the given path", %{dir: dir} do
    use_s3(version_plug(self(), md5_hex(@plain)))
    out = Path.join(dir, "out.db")

    Mix.Tasks.Fathom.Shard.run(["pull", "acme", out, "--version-id", @vid])

    assert_received {:get, "/b/acme.db", %{"versionId" => @vid}}
    assert File.read!(out) == @plain, "the written file must be the DECODED database"
    assert_received {:mix_shell, :info, [msg]}
    assert msg =~ "version #{@vid}"
  end

  test "without --version-id the current object is fetched (no versionId sent)", %{dir: dir} do
    use_s3(version_plug(self(), md5_hex(@plain)))
    out = Path.join(dir, "cur.db")

    # The stub 404s the current object; the normal pull reports it absent.
    assert_raise Mix.Error, ~r/no stored object/, fn ->
      Mix.Tasks.Fathom.Shard.run(["pull", "acme", out])
    end

    assert_received {:get, "/b/acme.db", params}
    refute Map.has_key?(params, "versionId")
  end

  test "an unknown version id is a clear error and writes nothing", %{dir: dir} do
    use_s3(version_plug(self(), md5_hex(@plain)))
    out = Path.join(dir, "missing.db")

    assert_raise Mix.Error, ~r/no version nope/, fn ->
      Mix.Tasks.Fathom.Shard.run(["pull", "acme", out, "--version-id", "nope"])
    end

    refute File.exists?(out)
  end

  test "an encoded version with no plaintext digest is refused", %{dir: dir} do
    use_s3(version_plug(self(), nil))
    out = Path.join(dir, "nodigest.db")

    assert {:error, {:missing_plain_digest, "zstd"}} =
             Storage.pull_object_version("acme", @vid, out)

    assert_raise Mix.Error, ~r/no fathom-md5 digest/, fn ->
      Mix.Tasks.Fathom.Shard.run(["pull", "acme", out, "--version-id", @vid])
    end

    refute File.exists?(out), "unverifiable decoded bytes were written"
  end

  test "a version whose decoded bytes mismatch the digest is refused, nothing written",
       %{dir: dir} do
    use_s3(version_plug(self(), md5_hex("some other database")))
    out = Path.join(dir, "bad.db")

    assert {:error, :checksum_mismatch} = Storage.pull_object_version("acme", @vid, out)
    refute File.exists?(out), "bytes that fail the digest check were promoted"

    assert_raise Mix.Error, ~r/checksum_mismatch/, fn ->
      Mix.Tasks.Fathom.Shard.run(["pull", "acme", out, "--version-id", @vid])
    end

    refute File.exists?(out)
  end

  test "a non-S3 backend fails clearly", %{dir: dir} do
    Application.put_env(:fathom, :shard_storage, Storage.Local)
    out = Path.join(dir, "local.db")

    assert {:error, {:object_versions_unsupported, Storage.Local}} =
             Storage.pull_object_version("acme", @vid, out)

    assert_raise Mix.Error, ~r/needs the S3 storage backend/, fn ->
      Mix.Tasks.Fathom.Shard.run(["pull", "acme", out, "--version-id", @vid])
    end

    refute File.exists?(out)
  end

  test "--version-id is rejected on other subcommands and when empty" do
    assert_raise Mix.Error, ~r/only applies to/, fn ->
      Mix.Tasks.Fathom.Shard.run(["inspect", "acme", "--version-id", @vid])
    end

    assert_raise Mix.Error, ~r/must not be empty/, fn ->
      Mix.Tasks.Fathom.Shard.run(["pull", "acme", "--version-id", ""])
    end
  end
end
