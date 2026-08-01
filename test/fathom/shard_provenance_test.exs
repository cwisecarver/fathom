defmodule Fathom.ShardProvenanceTest do
  @moduledoc """
  A local `.db` with no provenance was trusted as this node's own warm copy
  (expert review 2026-08-01 #2). One root cause, two triggers — and the second needs no
  attacker at all.

  Cold-open treats a present local file as a warm restart and skips the storage pull.
  `fork_evidence/2` mapped an absent sidecar to `:no_sidecar` and `resolve_fork/4` mapped that
  to `false` — "keep it, open warm" — after which `await_pull(nil, …)` adopted **whatever etag
  the store holds right now**, so the first fenced flush `If-Match`ed successfully.

    * **Trigger A (planted).** `ATTACH DATABASE '<data_dir>/<victim>.db'` *creates* its target,
      producing a structurally valid database for a tenant that has never been opened. When the
      victim's coordinator later starts, it serves the attacker's database and uploads it over
      the genuine object. (`286b530` closed the planting primitive; this is the second layer,
      and it is the layer that matters for a file arriving any other way.)

    * **Trigger B (no attacker).** A brand-new shard had no sidecar BY CONSTRUCTION —
      `promote_pull/2` explicitly `File.rm`'d it. So on a persisted `:shard_data_dir`: node A
      takes writes and dies before its first flush → the LB reroutes to B → B serves, flushes,
      idles and releases cleanly → A returns, sees its local file, opens warm, adopts the
      store's current etag, and its first flush destroys every write B acknowledged.

  The fix makes "no stored object" an explicit provenance claim (a sentinel sidecar) instead of
  an absence, and makes a genuinely absent sidecar fail closed.

  Not async: shards are global and back onto real files.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Storage}
  alias Filo.{Stmt, StmtResult}

  @sentinel "-"

  setup do
    shard = "prov_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Application.delete_env(:fathom, :adopt_unprovenanced_warm)
      Shards.drain(shard, 2_000)

      for dir <- [local_dir(), remote_dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(local_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp local_dir, do: Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
  defp local_db(shard), do: Path.join(local_dir(), "#{shard}.db")
  defp sidecar(shard), do: local_db(shard) <> ".etag"

  # Build a standalone db with one row, at `path`.
  defp seed_db(path, value) do
    File.mkdir_p!(Path.dirname(path))
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE IF NOT EXISTS kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{value}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok
  end

  # Put `value` into the STORED object, the way a peer node that flushed would.
  defp seed_remote(shard, value) do
    tmp = Path.join(System.tmp_dir!(), "prov_seed_#{System.unique_integer([:positive])}.db")
    seed_db(tmp, value)
    :ok = Storage.flush(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    :ok
  end

  defp served_value(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, %StmtResult{rows: rows}} = ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))
    :ok = ShardExecutor.close(conn)
    List.flatten(rows)
  end

  defp stored_value(shard) do
    dst = Path.join(System.tmp_dir!(), "prov_read_#{System.unique_integer([:positive])}.db")
    {:ok, _} = Storage.pull(shard, dst)
    {:ok, c} = Connection.open(dst)
    {:ok, %{rows: rows}} = Connection.query(c, "SELECT v FROM kv", [])
    :ok = Connection.close(c)
    for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s)
    List.flatten(rows)
  end

  defp quarantined?(shard), do: Path.wildcard(local_db(shard) <> ".forked.*") != []

  describe "trigger B — failover then fail-back, no attacker" do
    test "a node returning after a peer took over does not clobber the peer's writes",
         %{shard: shard} do
      # Node A: brand-new shard, wrote locally, died before its first flush. That file has the
      # sentinel because it was born against no stored object.
      seed_db(local_db(shard), "node_a_unflushed")
      File.write!(sidecar(shard), @sentinel)

      # Node B took over while A was down: served, flushed, released. The object now EXISTS.
      seed_remote(shard, "node_b_acked")

      # A comes back on its persisted data dir and opens the shard.
      log = capture_log(fn -> assert served_value(shard) == ["node_b_acked"] end)

      assert quarantined?(shard),
             "A's stale copy must be quarantined for recovery, not served"

      assert log =~ "an object now exists", "the quarantine must name the cause"

      # And crucially: B's acknowledged write is still the stored object.
      assert stored_value(shard) == ["node_b_acked"],
             "the returning node overwrote the lineage its peer acknowledged"
    end

    test "the quarantined copy is preserved, not deleted", %{shard: shard} do
      seed_db(local_db(shard), "node_a_unflushed")
      File.write!(sidecar(shard), @sentinel)
      seed_remote(shard, "node_b_acked")

      capture_log(fn -> served_value(shard) end)

      [preserved] = Path.wildcard(local_db(shard) <> ".forked.*")
      {:ok, c} = Connection.open(preserved)
      assert {:ok, %{rows: [["node_a_unflushed"]]}} = Connection.query(c, "SELECT v FROM kv", [])
      :ok = Connection.close(c)
    end

    test "a genuinely brand-new shard still opens warm and serves its own writes",
         %{shard: shard} do
      # The other half of the contract: the sentinel must not turn every new shard into a
      # quarantine. No stored object + sentinel provenance = our own file, serve it.
      seed_db(local_db(shard), "mine")
      File.write!(sidecar(shard), @sentinel)

      assert served_value(shard) == ["mine"]
      refute quarantined?(shard), "a brand-new shard with no stored object must open warm"
    end
  end

  describe "trigger A — a file with no provenance at all" do
    test "an unprovenanced local file is quarantined, not adopted as authoritative",
         %{shard: shard} do
      # The genuine tenant lineage.
      seed_remote(shard, "genuine")

      # A file that fathom did not write here — planted, or a pre-provenance legacy copy.
      seed_db(local_db(shard), "planted")
      File.rm(sidecar(shard))

      log = capture_log(fn -> assert served_value(shard) == ["genuine"] end)

      assert quarantined?(shard), "an unknown-lineage file must not be served"
      assert log =~ "no provenance sidecar"

      assert stored_value(shard) == ["genuine"],
             "the planted file was uploaded over the genuine object"
    end

    test "an unprovenanced file does not become the shard when there is no stored object",
         %{shard: shard} do
      # Nothing legitimate exists yet. A planted file must still not be adopted — this is the
      # "shard that has never been opened" case, where the plant would BECOME the tenant.
      seed_db(local_db(shard), "planted")
      File.rm(sidecar(shard))

      capture_log(fn ->
        {:ok, conn} = ShardExecutor.open(shard)
        # The table came from the planted file; a fresh shard has no such table.
        assert {:error, _} = ShardExecutor.execute(conn, stmt("SELECT v FROM kv"))
        :ok = ShardExecutor.close(conn)
      end)

      assert quarantined?(shard)
    end

    test ":adopt_unprovenanced_warm restores the old behaviour for a legacy fleet",
         %{shard: shard} do
      Application.put_env(:fathom, :adopt_unprovenanced_warm, true)
      seed_remote(shard, "genuine")
      seed_db(local_db(shard), "legacy_local")
      File.rm(sidecar(shard))

      log = capture_log(fn -> assert served_value(shard) == ["legacy_local"] end)

      refute quarantined?(shard)
      assert log =~ "adopting", "the escape hatch must say loudly what it is doing"
    end
  end

  describe "the sentinel is written where a real shard is born" do
    test "a shard created through the normal path carries provenance", %{shard: shard} do
      # No stored object, no local file: the ordinary birth of a new tenant.
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('born')"))
      :ok = ShardExecutor.close(conn)

      assert File.read!(sidecar(shard)) == @sentinel,
             "a born-empty shard must record 'derived from no object', not nothing at all"
    end

    test "the first flush of a born-empty shard can only CREATE, never overwrite",
         %{shard: shard} do
      # Provenance nil ⇒ If-None-Match:* at the storage layer. If a peer created the object
      # first, our flush must be refused rather than clobber it.
      seed_db(local_db(shard), "ours")
      File.write!(sidecar(shard), @sentinel)

      {:ok, pid, ref, _path} = Shards.checkout(shard)
      assert :sys.get_state(pid).etag == nil, "a sentinel warm open must fence with nil"
      Shard.checkin(pid, ref)
    end
  end
end
