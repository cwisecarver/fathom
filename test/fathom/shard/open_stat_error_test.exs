defmodule Fathom.Shard.OpenStatErrorTest do
  @moduledoc """
  Expert review 2026-11-07 #21: the warm-vs-cold decision is a stat, and only a DEFINITE ENOENT may
  mean "cold".

  `Fathom.FS.exists?/1` maps every `{:error, _}` to `false`, so an EIO / EACCES / ESTALE at exactly
  the open's stat read as "no local file": no fork check, `warm? = false`, a cold pull — and
  `Materializer.promote_pull` then unlinks `<id>.db-wal`, `-shm` and the `.db` before landing the
  pulled object. Acked-unflushed tenant writes destroyed by a stat hiccup, with no quarantine copy.

  ## The fixture

  A real filesystem rarely gives an unprivileged test a stat failure on one name only (a directory
  the stat cannot search is also one the pull cannot write, so the unfixed code would fail the open
  for the wrong reason and pass quietly). A SYMLINK LOOP does: `stat(<id>.db)` answers `ELOOP` while
  every sibling — the `-wal` holding the acked frames, the sidecar, the pull temp — behaves normally.
  The unfixed build reads ELOOP as "no file", cold-pulls, and removes the `-wal`; the fixed build
  fails the open before the lease and before any pull, and every byte is left where it was.

  The same predicate guards `Retained.unflushed_local?/1` (an indeterminate stat must read "may hold
  unflushed writes", not "nothing to protect").

  Both liveness modes, mode asserted on the control open.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat, Materializer, Provenance, Retained}
  alias Fathom.Shard.Storage.Local

  @modes [:legacy, :heartbeat]

  @env [
    :shard_storage,
    :shard_data_dir,
    :replication_enabled,
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :shard_idle_ms,
    :shard_flush_interval_ms
  ]

  setup do
    shard = "statfail_#{System.unique_integer([:positive])}"
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})
    dir = Path.join(System.tmp_dir!(), "fathom_statfail_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      Shards.drain(shard, 2_000)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(dir)
      Local.purge_shard(shard)
    end)

    Application.put_env(:fathom, :shard_data_dir, dir)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)
    Application.put_env(:fathom, :replication_enabled, false)
    Application.put_env(:fathom, :replication_promote_on_open, false)
    Application.put_env(:fathom, :replication_recover_from_peers, false)

    %{shard: shard, db: Shard.db_path(shard)}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?(), "legacy mode needs the heartbeat OFF")

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp put_object!(shard, v) do
    tmp = Path.join(System.tmp_dir!(), "statfail_seed_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(tmp)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{v}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Local.flush(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    {:ok, etag} = Local.object_etag(shard)
    etag
  end

  # `<id>.db` whose stat fails (ELOOP) beside a real `-wal` and sidecar — the dirty warm copy a
  # stat hiccup would hide.
  defp stage_unstattable_warm!(db, etag) do
    wal = db <> "-wal"
    wal_bytes = :crypto.strong_rand_bytes(4_096)
    :ok = File.ln_s(db, db)
    File.write!(wal, wal_bytes)
    :ok = Provenance.write(db, etag)

    assert File.stat(db) == {:error, :eloop},
           "fixture: the stat of the .db must fail with something other than ENOENT"

    refute Fathom.FS.exists?(db), "fixture: this is exactly what `exists?/1` collapsed to `false`"
    assert File.read!(wal) == wal_bytes
    {wal, wal_bytes}
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do
        set_mode!(unquote(mode))
        :ok
      end

      test "an indeterminate stat of the .db fails the open and destroys nothing",
           %{shard: shard, db: db} do
        e0 = put_object!(shard, "e0")
        {wal, wal_bytes} = stage_unstattable_warm!(db, e0)
        sidecar = File.read!(Provenance.sidecar_path(db))

        {result, log} = with_log(fn -> ShardExecutor.open(shard) end)

        assert File.exists?(wal) and File.read!(wal) == wal_bytes,
               "the cold pull deleted the dirty warm copy's -wal (acked-unflushed frames)"

        assert {:error, _} = result, "the open served despite not knowing whether a copy exists"

        assert File.read_link(db) == {:ok, db}, "the .db was unlinked or replaced"
        assert File.read!(Provenance.sidecar_path(db)) == sidecar
        refute File.exists?(Materializer.pull_temp(db)), "a pull ran despite the failed stat"
        assert log =~ "cannot tell whether a local copy exists"
        assert Local.object_etag(shard) == {:ok, e0}
      end

      # FS.exists? audit, batch D follow-up. `PromoteOnOpen.purge_if_recovering/1` read an
      # indeterminate stat of the `.recovering` marker as "no marker", so a previous open that died
      # between installing its pulled object and finishing the peer ask had its UNVETTED pull served
      # warm (the peers that acked newer writes were never asked). It must refuse, not guess: reading
      # it as "found" would delete a copy that may hold acked writes.
      test "an indeterminate stat of the .recovering marker fails the open and touches nothing",
           %{shard: shard, db: db} do
        e0 = put_object!(shard, "e0")
        # A REAL database that matches the stored object and carries its sidecar: the unfixed open
        # adopts it warm and serves it (a random-bytes fixture failed the open for an unrelated
        # reason and discriminated only on the log line).
        {:ok, c} = Connection.open(db)
        :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
        :ok = Connection.exec(c, "INSERT INTO kv VALUES ('e0')")
        :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
        Connection.close(c)
        for s <- ["-wal", "-shm"], do: File.rm(db <> s)
        bytes = File.read!(db)
        :ok = Provenance.write(db, e0)
        sidecar = File.read!(Provenance.sidecar_path(db))

        marker = Fathom.Shard.PromoteOnOpen.recovering_path(db)
        :ok = File.ln_s(marker, marker)

        assert File.stat(marker) == {:error, :eloop},
               "fixture: the marker's stat must fail with something other than ENOENT"

        refute Fathom.FS.exists?(marker), "fixture: exactly what `exists?/1` collapsed to `false`"

        {result, log} = with_log(fn -> ShardExecutor.open(shard) end)

        assert {:error, _} = result, "the open served (or opened) despite not knowing the marker"
        assert File.read!(db) == bytes, "the copy was served, quarantined or replaced"
        assert File.read!(Provenance.sidecar_path(db)) == sidecar
        assert File.read_link(marker) == {:ok, marker}
        refute File.exists?(Materializer.pull_temp(db)), "a pull ran despite the failed stat"
        assert log =~ "cannot tell whether a previous open left a recovery marker"
        assert Local.object_etag(shard) == {:ok, e0}
      end

      test "a definite stat answer opens as before (control: the fix does not fail every open)",
           %{shard: shard, db: db} do
        # ENOENT is a definite answer: the cold pull proceeds, the mode took, the sidecar names it.
        e0 = put_object!(shard, "e0")
        {:ok, conn} = ShardExecutor.open(shard)
        {:ok, _} = ShardExecutor.execute(conn, %Filo.Stmt{sql: "SELECT 1", args: []})
        :ok = ShardExecutor.close(conn)

        {:ok, pid} = Shards.ensure(shard)

        case unquote(mode) do
          :heartbeat -> assert is_integer(:sys.get_state(pid).acquire_gen)
          :legacy -> assert is_nil(:sys.get_state(pid).acquire_gen)
        end

        assert File.regular?(db)
        assert Provenance.read(db) == {:ok, e0}
      end
    end
  end

  test "Retained.unflushed_local?/1 reads an indeterminate stat as 'may hold unflushed writes'",
       %{db: db} do
    :ok = File.ln_s(db, db)
    assert File.stat(db) == {:error, :eloop}

    assert Retained.unflushed_local?(db),
           "an unstattable .db read as 'no local copy': a drain would proceed past a dirty one"

    # …and a definitely-absent file is still "nothing to protect".
    refute Retained.unflushed_local?(db <> ".never-existed")
  end
end
