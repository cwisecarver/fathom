defmodule Fathom.Shard.Storage.S3PlainSha256Test do
  @moduledoc """
  The pull verifies the decoded bytes with SHA-256 when the object carries
  `x-amz-meta-fathom-sha256`, and with MD5 otherwise (expert review 2026-10-20-2027 #23).

  The MD5 (`x-amz-meta-fathom-md5`) stays the object's identity — PUT intents and `Fork` compare it —
  so every compressed flush still writes it; the SHA-256 is only a cheaper verify hash for the pull.
  The tests prove WHICH hash a pull checks by corrupting one header and leaving the other right.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  @md5 "x-amz-meta-fathom-md5"
  @sha "x-amz-meta-fathom-sha256"

  setup do
    dir = Path.join(System.tmp_dir!(), "s3sha_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    prev = for k <- [S3, :s3_plain_sha256], do: {k, Application.get_env(:fathom, k)}
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    on_exit(fn ->
      File.rm_rf!(dir)

      for {k, v} <- prev,
          do:
            if(is_nil(v),
              do: Application.delete_env(:fathom, k),
              else: Application.put_env(:fathom, k, v)
            )
    end)

    local = Path.join(dir, "src.db")
    plain = :binary.copy("fathom shard bytes ", 20_000)
    File.write!(local, plain)
    %{store: store, dir: dir, local: local, plain: plain}
  end

  defp hex(alg, bin), do: Base.encode16(:crypto.hash(alg, bin), case: :lower)

  defp set_meta(store, key, k, v) do
    Agent.update(store, fn s ->
      update_in(s.objects[key].meta, &Map.put(&1, k, v))
    end)
  end

  test "a compressed flush labels the plaintext with BOTH the MD5 identity and a SHA-256",
       %{store: store, local: local, plain: plain} do
    assert {:ok, _, _} = S3.flush("sha1", local, nil)
    meta = S3EtagStore.meta_of(store, "sha1.db")
    assert meta[@md5] == hex(:md5, plain), "the identity hash changed"
    assert meta[@sha] == hex(:sha256, plain)
  end

  test "an object with the SHA-256 header is verified by SHA-256, not MD5",
       %{store: store, dir: dir, local: local, plain: plain} do
    assert {:ok, _, _} = S3.flush("sha2", local, nil)
    dst = Path.join(dir, "dst.db")

    assert {:ok, _} = S3.pull("sha2", dst)
    assert File.read!(dst) == plain
    File.rm!(dst)

    # A wrong MD5 with a right SHA-256 still pulls: the MD5 is not what is checked...
    set_meta(store, "sha2.db", @md5, hex(:md5, "something else"))
    assert {:ok, _} = S3.pull("sha2", dst)
    File.rm!(dst)

    # ...and a wrong SHA-256 is refused even with the MD5 restored.
    set_meta(store, "sha2.db", @md5, hex(:md5, plain))
    set_meta(store, "sha2.db", @sha, hex(:sha256, "something else"))
    assert {:error, :checksum_mismatch} = S3.pull("sha2", dst)
    refute File.exists?(dst)
  end

  test "an object without the SHA-256 header falls back to the MD5 check",
       %{store: store, dir: dir, local: local} do
    Application.put_env(:fathom, :s3_plain_sha256, false)
    assert {:ok, _, _} = S3.flush("sha3", local, nil)
    refute Map.has_key?(S3EtagStore.meta_of(store, "sha3.db"), @sha)

    dst = Path.join(dir, "dst.db")
    assert {:ok, _} = S3.pull("sha3", dst)
    File.rm!(dst)

    set_meta(store, "sha3.db", @md5, hex(:md5, "something else"))
    assert {:error, :checksum_mismatch} = S3.pull("sha3", dst)
  end

  test "an encoded object with a SHA-256 but no MD5 identity is still refused",
       %{store: store, dir: dir, local: local} do
    assert {:ok, _, _} = S3.flush("sha4", local, nil)
    Agent.update(store, fn s -> update_in(s.objects["sha4.db"].meta, &Map.delete(&1, @md5)) end)
    assert {:error, {:missing_plain_digest, _}} = S3.pull("sha4", Path.join(dir, "dst.db"))
  end
end
