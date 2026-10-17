defmodule Fathom.Shard.SidecarStampUnsyncedTest do
  @moduledoc """
  The periodic flush's post-PUT sidecar stamp is a `pwrite` with NO fsync (expert review 2026-10-17
  #32), so a flush costs ONE sidecar fsync (the pre-PUT intent) instead of two.

  ## Why that is safe — and what this file proves

  After an OS crash the sidecar on disk can be no older than its last FSYNCED write. The only
  fsynced write of a flush is the pre-PUT intent record: the previous etag plus the md5 of the bytes
  about to be PUT. `Fork` adopts a stored object whose md5 matches a recorded intent, so a lost stamp
  is the mailbox-gap state recovery already handles — adoption, not quarantine.

  The risk is a SECOND flush: if the file could be left behind its own PUT by more than one
  generation, the surviving intent would name neither object. It cannot, because flush N+1's intent
  record is an fsync of the SAME file (it persists whatever stamp N left in the page cache) and
  completes before flush N+1's PUT. Concretely:

    * **Invariant (`durable image`)** — at the instant each PUT lands, the last fsynced image of the
      sidecar already names that object (by etag, or by an intent equal to its md5). Checked over
      several consecutive flushes, plus that each flush now makes exactly one sidecar fsync (the
      pre-fix code made two, so this fails against it).
    * **Crash-injection** — "the OS crashed" is staged as: kill the coordinator, overwrite the
      sidecar with the last fsynced image (exactly what the disk would hold), reopen. The reopen
      must ADOPT the node's own landed object: nothing quarantined, every acked row present, and the
      sidecar re-stamped with the object's etag. Run after one flush and after two consecutive ones
      (the torn/reverted case), in both liveness modes.
    * **Negative control** — an image TWO generations old (what the disk would hold if the intent
      were not fsynced before the PUT) does quarantine. It shows the crash-injection can fail and
      pins WHY the pre-PUT fsync must stay.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Provenance, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "unsynced_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)

    on_exit(fn ->
      Application.delete_env(:fathom, :provenance_write_hook)
      restore(:shard_storage, prev_storage)
      restore(:shard_flush_interval_ms, prev_flush)
      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock", ".pos", ".lineage"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp db(shard), do: Shard.db_path(shard)

  defp set_mode!(:legacy), do: refute(Heartbeat.running?(), "legacy mode needs the heartbeat OFF")

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(coordinator, :heartbeat),
    do: assert(is_integer(:sys.get_state(coordinator).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(coordinator, :legacy),
    do: assert(is_nil(:sys.get_state(coordinator).acquire_gen), "legacy mode did not take")

  # Record, in order, (a) every fsynced in-place sidecar image and (b) the instant each flush PUT has
  # landed (just before the unsynced stamp), with the stored object's etag + md5 at that instant.
  defp capture!(shard) do
    test_pid = self()
    path = db(shard)

    Application.put_env(:fathom, :provenance_write_hook, fn
      :synced, ^path ->
        send(test_pid, {:synced, File.read!(path <> ".etag")})

      :flush_stamp, ^path ->
        case Storage.object_head(shard) do
          {:ok, %{etag: etag, md5: md5}} -> send(test_pid, {:put_landed, etag, md5})
          _ -> :ok
        end

      _stage, _path ->
        :ok
    end)
  end

  defp drain_events(acc \\ []) do
    receive do
      {:synced, _} = e -> drain_events([e | acc])
      {:put_landed, _, _} = e -> drain_events([e | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # Does this sidecar IMAGE name the object `etag`/`md5` — by etag, or by a recorded intent?
  defp image_names?(image, etag, md5) do
    probe = Path.join(System.tmp_dir!(), "sidecar_image_#{System.unique_integer([:positive])}.db")
    File.write!(probe <> ".etag", image)

    try do
      Provenance.read(probe) == {:ok, etag} or md5 in Provenance.read_intents(probe)
    after
      File.rm(probe <> ".etag")
    end
  end

  # Open `shard` in `mode`, take the first write + first flush, then run `rounds` more
  # write-then-flush rounds. Returns the per-round events and the coordinator/conn.
  defp run_rounds!(shard, mode, rounds) do
    set_mode!(mode)
    capture!(shard)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, coordinator} = Shards.ensure(shard)
    assert_mode!(coordinator, mode)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('r0')"))

    # The first-write fsync is a task; let its image land before the first flush's events.
    case :sys.get_state(coordinator).sidecar_task do
      %Task{pid: pid} ->
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

      nil ->
        :ok
    end

    :ok = Shards.flush(shard)
    warmup = drain_events()

    per_round =
      for i <- 1..rounds do
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('r#{i}')"))
        :ok = Shards.flush(shard)
        drain_events()
      end

    %{conn: conn, coordinator: coordinator, warmup: warmup, per_round: per_round}
  end

  # "The OS crashed": kill the coordinator, put the sidecar back to `image` (what the disk holds),
  # then reopen and read everything back. Returns the reopen's log.
  defp crash_and_reopen!(shard, %{conn: conn, coordinator: coordinator}, image) do
    ref = Process.monitor(coordinator)
    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :killed}, 2_000
    _ = :sys.get_state(Fathom.ShardSupervisor)
    ShardExecutor.close(conn)
    File.write!(db(shard) <> ".etag", image)
    Application.delete_env(:fathom, :provenance_write_hook)

    capture_log(fn ->
      {:ok, conn2} = ShardExecutor.open(shard)

      {:ok, %Filo.StmtResult{rows: rows}} =
        ShardExecutor.execute(conn2, stmt("SELECT v FROM kv ORDER BY v"))

      send(self(), {:rows, List.flatten(rows) |> Enum.map(&to_string/1)})
      ShardExecutor.close(conn2)
    end)
    |> then(fn log ->
      assert_received {:rows, rows}
      {log, rows}
    end)
  end

  defp last_image(events) do
    events |> Enum.filter(&match?({:synced, _}, &1)) |> List.last() |> elem(1)
  end

  for mode <- @modes do
    describe "post-PUT stamp without fsync (#{mode})" do
      test "at every PUT the last fsynced image already names the object; one fsync per flush",
           %{shard: shard} do
        %{per_round: per_round, warmup: warmup} = run_rounds!(shard, unquote(mode), 3)

        # Walk ALL events in order, carrying the last durable image across rounds.
        all = warmup ++ Enum.concat(per_round)

        Enum.reduce(all, nil, fn
          {:synced, image}, _last ->
            image

          {:put_landed, etag, md5}, last ->
            assert last != nil, "a PUT landed before any sidecar image was fsynced"

            assert image_names?(last, etag, md5),
                   "the PUT landed on #{etag} (md5 #{md5}) but the last fsynced sidecar image " <>
                     "names neither: a crash now would quarantine acked writes"

            last
        end)

        for {events, i} <- Enum.with_index(per_round, 1) do
          assert Enum.count(events, &match?({:synced, _}, &1)) == 1,
                 "flush round #{i} made #{inspect(Enum.count(events, &match?({:synced, _}, &1)))} " <>
                   "sidecar fsyncs; the pre-PUT intent is the only one the flush needs"

          assert Enum.count(events, &match?({:put_landed, _, _}, &1)) == 1
        end
      end

      for rounds <- [1, 2] do
        test "OS crash after #{rounds} flush(es): the reopen adopts its own landed object",
             %{shard: shard} do
          ctx = run_rounds!(shard, unquote(mode), unquote(rounds))
          {:ok, object_etag} = Storage.object_etag(shard)
          image = last_image(ctx.warmup ++ Enum.concat(ctx.per_round))

          # Precondition: the staged crash really is the interesting state — the durable sidecar is
          # BEHIND the object (it lost the stamp), so the reopen has to take the adoption route.
          probe = db(shard) <> ".probe"
          File.write!(probe <> ".etag", image)
          refute Provenance.read(probe) == {:ok, object_etag}, "fixture: the image is not stale"
          File.rm(probe <> ".etag")

          {log, rows} = crash_and_reopen!(shard, ctx, image)

          assert log =~ "adopted", "the reopen did not adopt the node's own landed object"
          assert Path.wildcard(db(shard) <> ".forked.*") == [], "a good copy was quarantined"

          assert rows == Enum.map(0..unquote(rounds), &"r#{&1}"),
                 "an acked row was lost across the crash"

          assert {:ok, ^object_etag} = Provenance.read(db(shard))
        end
      end
    end
  end

  # NEGATIVE CONTROL. Two generations behind (what the disk would hold if the intent were NOT
  # fsynced before the PUT, so only the previous round's image survived): the surviving intent names
  # neither the object nor ... the object, and `Fork` correctly refuses to adopt a stranger's bytes.
  # This is the failure the pre-PUT fsync exists to prevent; it is why only the POST-PUT one went.
  test "an image two flushes stale is quarantined (why the pre-PUT fsync must stay)", %{
    shard: shard
  } do
    ctx = run_rounds!(shard, :legacy, 2)
    synced = (ctx.warmup ++ Enum.concat(ctx.per_round)) |> Enum.filter(&match?({:synced, _}, &1))
    assert length(synced) >= 3
    {:synced, stale} = Enum.at(synced, -3)

    {log, _rows} = crash_and_reopen!(shard, ctx, stale)

    refute log =~ "adopted"
    assert Path.wildcard(db(shard) <> ".forked.*") != [], "the stale image was not a fork"
  end
end
