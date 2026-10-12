defmodule Fathom.Directory.ReconcileTest do
  @moduledoc """
  Cross-store DR reconcile (expert review 2026-07-19 #6): after a Postgres directory restore rewinds
  it out of sync with storage, the sweep realigns the directory to the authoritative durable facts —
  the file's `user_version` (schema_version), the storage revocation floor (token_version), and the
  storage tombstones (deleted status). Directory is sandboxed (DataCase); storage is real files, so
  each test seeds the stored object directly and cleans up the storage keys.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Directory
  alias Fathom.Directory.Reconcile
  alias Fathom.Shard.{Connection, Storage}

  defp uniq, do: "recon_#{System.unique_integer([:positive])}"

  # Build a real SQLite file at `user_version` and push it as the shard's stored object (no
  # coordinator needed — this is the durable object reconcile reads).
  defp seed_object(shard, user_version) do
    tmp = Path.join(System.tmp_dir!(), "seed_#{shard}_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(tmp)
    {:ok, _} = Connection.query(conn, "PRAGMA user_version = #{user_version}", [])
    {:ok, _} = Connection.query(conn, "CREATE TABLE t (x)", [])
    :ok = Connection.close(conn)
    :ok = Storage.flush(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
  end

  defp cleanup(shard) do
    for s <- [".db", ".db-wal", ".db-shm", ".lock"],
        do: File.rm(Path.join(remote_dir(), shard <> s))

    File.rm(Path.join([remote_dir(), "tokenfloors", shard]))
    File.rm(Path.join([remote_dir(), "tombstones", shard]))
  end

  test "aligns directory schema_version to the file's user_version; dry run reports, --fix applies (#6)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 5)

    # Dry run reports the drift and changes nothing (the directory was rolled back below the file).
    findings = Reconcile.run()

    assert Enum.any?(
             findings,
             &match?(%{shard_id: ^shard, kind: :schema_drift, from: 0, to: 5, fixed: false}, &1)
           )

    assert {:ok, %{schema_version: 0}} = Directory.get(shard),
           "dry run must not modify the directory"

    # --fix aligns the directory to the file's authoritative version.
    fixed = Reconcile.run(fix: true)

    assert Enum.any?(
             fixed,
             &match?(%{shard_id: ^shard, kind: :schema_drift, to: 5, fixed: true}, &1)
           )

    assert {:ok, %{schema_version: 5}} = Directory.get(shard)
  end

  test "raises token_version to the durable storage floor (#6)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 0)
    :ok = Storage.put_token_floor(shard, 7)

    _ = Reconcile.run(fix: true)

    assert Directory.token_version(shard) >= 7,
           "the directory floor must be raised to the storage floor"
  end

  test "re-tombstones a storage-tombstoned id the directory forgot (#6)" do
    shard = uniq()
    on_exit(fn -> cleanup(shard) end)
    # The directory says active (a restore un-deleted it), but storage says deleted.
    {:ok, _} = Directory.resolve(shard)
    seed_object(shard, 0)
    :ok = Storage.put_tombstone(shard)

    _ = Reconcile.run(fix: true)
    assert {:ok, %{status: "deleted"}} = Directory.get(shard)
  end

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()

  # Expert review 2026-10-12 #19: the sweep pulled every WHOLE object serially to read four header
  # bytes. On the S3 req_plug seam, prove the version now comes from a RANGED read and no GET of the
  # object body is ever made.
  describe "S3: no whole-object pull (expert review 2026-10-12 #19)" do
    alias Fathom.Shard.Storage.S3

    setup do
      prev_store = Application.get_env(:fathom, :shard_storage)
      prev_s3 = Application.get_env(:fathom, S3)

      on_exit(fn ->
        if prev_store,
          do: Application.put_env(:fathom, :shard_storage, prev_store),
          else: Application.delete_env(:fathom, :shard_storage)

        if prev_s3,
          do: Application.put_env(:fathom, S3, prev_s3),
          else: Application.delete_env(:fathom, S3)
      end)
    end

    test "reads user_version with a ranged GET only and reports the drift" do
      shard = uniq()
      {:ok, _} = Directory.resolve(shard)
      test_pid = self()

      # A valid SQLite header (magic + user_version=9 at offset 60) then 1 MiB of filler.
      object =
        <<"SQLite format 3\0", 0::size(352), 9::signed-big-32, 0::size(288)>> <>
          :crypto.strong_rand_bytes(1_048_576)

      plug = fn conn ->
        range = Plug.Conn.get_req_header(conn, "range")
        send(test_pid, {:req, conn.method, conn.request_path, range})

        cond do
          conn.query_string =~ "list-type" ->
            Plug.Conn.send_resp(
              conn,
              200,
              ~s(<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>)
            )

          conn.request_path =~ ~r{tokenfloors|tombstones} ->
            Plug.Conn.send_resp(conn, 404, "")

          range == [] ->
            Plug.Conn.send_resp(conn, 200, object)

          true ->
            ["bytes=0-" <> last] = range
            body = binary_part(object, 0, min(byte_size(object), String.to_integer(last) + 1))
            Plug.Conn.send_resp(conn, 206, body)
        end
      end

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

      findings = Reconcile.run()

      assert Enum.any?(
               findings,
               &match?(%{shard_id: ^shard, kind: :schema_drift, from: 0, to: 9}, &1)
             )

      reqs = collect_reqs([])

      assert Enum.any?(reqs, fn {m, p, r} ->
               m == "GET" and String.ends_with?(p, shard <> ".db") and r != []
             end)

      refute Enum.any?(reqs, fn {m, p, r} ->
               m == "GET" and String.ends_with?(p, shard <> ".db") and r == []
             end),
             "reconcile made a full-body GET of the object: #{inspect(reqs)}"
    end

    defp collect_reqs(acc) do
      receive do
        {:req, m, p, r} -> collect_reqs([{m, p, r} | acc])
      after
        0 -> acc
      end
    end
  end
end
