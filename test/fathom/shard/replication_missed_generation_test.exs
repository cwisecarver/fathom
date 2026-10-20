defmodule Fathom.Shard.ReplicationMissedGenerationTest do
  @moduledoc """
  A follower that misses a WHOLE WAL generation must not absorb its way back to "complete"
  (round-4 replication review 2026-10-20).

  ## The bug

  `prev_extent` on a reset push tells a follower how far the generation it is about to discard got
  (`Session.outgoing_extent/2`). It measured the follower against ITS OWN generation only: peers
  still in that generation plus the Session's own read extent for it. A follower still sitting in
  generation g-1 when the primary is already in g+1 — down, or disconnected, for the whole of g —
  held all of g-1, so it was told "complete", absorbed its g-1 WAL, cleared `torn` and appended
  g+1. Generation g's pages are in neither of its files: they reach it again only if a later
  generation happens to rewrite them.

  The replica then opens cleanly, passes `quick_check`, ranks EQUAL to its peers (same lineage,
  ordinal and offset) and is promotable — so a failover that picks it (its node is the LB's
  survivor and short-circuits on a tie, or it is the one peer that answers) serves a database
  missing acked writes. One follower outage across one checkpoint is enough; no second fault.

  ## The rule under test

  A follower whose generation is not the one immediately before the current header, among the
  generations the Session has read, missed at least one generation with frames and is SHORT: the
  reset says so, and the follower asks for a seed instead of absorbing.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Fleet, Follower, Session, Shipper, Wal}
  alias Fathom.Shards

  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_ordinal_wire,
    :replication_catchup_ms
  ]

  setup do
    id = "repl_missgen_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replmissgen_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})

    # Prod's wire: the push states lineage + ordinal, so the restarted follower sees the SAME
    # ownership (equal lineage) and takes the absorb path rather than an ownership re-seed.
    Application.put_env(:fathom, :replication_ordinal_wire, true)
    # The disconnected follower's catch-up retries must not race the test's own steps.
    Application.put_env(:fathom, :replication_catchup_ms, 60_000)

    on_exit(fn ->
      Session.stop(id)

      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)

      Shards.drain(id, 5_000)
      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp start_follower!(root, name, port) do
    dir = Path.join(root, to_string(name))
    pid = start_supervised!({Follower, name: name, port: port, dir: dir}, id: name)
    {:ok, port} = Follower.port(pid)
    {name, port}
  end

  defp await(what, fun, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if fun.(), do: :ok, else: Process.sleep(25)
    end)
    |> Enum.find(fn
      :ok -> true
      _ -> System.monotonic_time(:millisecond) > deadline
    end)
    |> case do
      :ok -> :ok
      _ -> flunk("timed out waiting for #{what}")
    end
  end

  defp commit!(s), do: assert(:ok = Session.commit(s.id, s.wal, s.coordinator))

  defp write!(s, sql) do
    {:ok, _} = Connection.query(s.conn, sql, [])
    commit!(s)
  end

  defp header(s) do
    {:ok, h} = Wal.read(s.wal)
    h
  end

  # A checkpoint of the whole WAL on a handle of the test's own, then a write: SQLite restarts the
  # log in place at that write (new salt, ckpt_seq + 1) — one generation boundary, deterministic.
  # Every follower that is UP holds the generation being folded (asserted by the caller), so this
  # stands in for a covered checkpoint.
  defp next_generation!(s, sql) do
    before = header(s)
    {:ok, raw} = Exqlite.Sqlite3.open(s.path)

    try do
      {:ok, stmt} = Exqlite.Sqlite3.prepare(raw, "PRAGMA wal_checkpoint(PASSIVE)")
      {:row, [busy, log, done]} = Exqlite.Sqlite3.step(raw, stmt)
      :ok = Exqlite.Sqlite3.release(raw, stmt)
      assert busy == 0 and log == done, "precondition: the checkpoint did not fold the whole WAL"
    after
      Exqlite.Sqlite3.close(raw)
    end

    write!(s, sql)
    after_ = header(s)
    assert after_.salt1 != before.salt1, "precondition: the write did not restart the WAL"
    after_
  end

  defp holds?(name, id, %{ckpt_seq: g, salt1: salt, commit_extent: e}) do
    match?(%{wal_gen: ^g, salt1: ^salt, next_offset: ^e}, Follower.state_of(name, id))
  end

  # What the replica's own files answer, read from a COPY so nothing here checkpoints them.
  defp replica_query(root, name, id, sql) do
    dir = Path.join(root, "copy_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    db = Path.join(dir, "r.db")
    File.cp!(Follower.db_path(name, id), db)
    wal = Follower.wal_path(name, id)
    if File.exists?(wal), do: File.cp!(wal, db <> "-wal")
    {:ok, conn} = Exqlite.Sqlite3.open(db)

    try do
      with {:ok, stmt} <- Exqlite.Sqlite3.prepare(conn, sql) do
        r = Exqlite.Sqlite3.step(conn, stmt)
        Exqlite.Sqlite3.release(conn, stmt)
        r
      end
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  # `later` generations after g: 1 keeps F's generation inside the Session's `seen` horizon (4);
  # 5 pushes it past, where F's generation is not listed at all.
  for later <- [1, 5] do
    @tag later: later
    test "a follower down across #{later + 1} generation(s) is re-seeded, not absorbed to 'complete'",
         %{id: id, root: root, later: later} do
      a = start_follower!(root, :"mg_a_#{System.unique_integer([:positive])}", 0)
      b = start_follower!(root, :"mg_b_#{System.unique_integer([:positive])}", 0)
      {f, f_port} = start_follower!(root, :"mg_f_#{System.unique_integer([:positive])}", 0)
      followers = [a, b, {f, f_port}]

      Application.put_env(:fathom, :replication_enabled, true)
      Application.put_env(:fathom, :replication_quorum, 2)

      Application.put_env(
        :fathom,
        :replication_followers,
        for({_n, port} <- followers, do: {~c"127.0.0.1", port})
      )

      start_supervised!(Fleet)
      await("shippers to connect", fn -> Enum.all?(Fleet.shippers(), &Shipper.connected?/1) end)

      {:ok, coordinator, ref, path} = Shards.checkout(id)
      on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
      {:ok, conn} = Connection.open(path)
      on_exit(fn -> Connection.close(conn) end)
      s = %{id: id, conn: conn, coordinator: coordinator, path: path, wal: path <> "-wal"}

      # GENERATION g-1: every follower seeded and holding all of it.
      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER)", [])
      write!(s, "INSERT INTO t VALUES (1)")
      g0 = header(s)

      await("every follower to hold generation g-1", fn ->
        _ = Session.commit(id, s.wal, coordinator)
        Enum.all?(followers, fn {n, _} -> holds?(n, id, g0) end)
      end)

      # F goes down holding ALL of g-1 — it is not short in its own generation.
      stop_supervised!(f)

      # GENERATION g: the acked write F never sees. A new table: its schema row lives on page 1 and
      # its row on a fresh page, and nothing later in this test rewrites either.
      g1 = next_generation!(s, "CREATE TABLE lost (v TEXT)")
      write!(s, "INSERT INTO lost VALUES ('acked-in-g')")
      g1 = %{g1 | commit_extent: header(s).commit_extent}

      for {n, _} <- [a, b] do
        await("#{n} to hold generation g", fn -> holds?(n, id, g1) end)
      end

      # GENERATIONS g+1.., still without F. Each touches only `t`'s page.
      for i <- 1..later, do: next_generation!(s, "INSERT INTO t VALUES (#{100 + i})")

      # F comes back on the same port with its g-1 files, and the next commit reaches it with a reset.
      {^f, ^f_port} = start_follower!(root, f, f_port)

      await("F's shipper to reconnect", fn ->
        Enum.all?(Fleet.shippers(), &Shipper.connected?/1)
      end)

      write!(s, "INSERT INTO t VALUES (3)")

      # Converge: F either absorbs (pre-fix) or is re-seeded (post-fix). Either way it ends untorn at
      # the primary's position — the state in which promotion trusts it.
      await("F to converge, untorn, at the primary's position", fn ->
        _ = Session.commit(id, s.wal, coordinator)
        st = Follower.state_of(f, id)
        st != nil and st.torn == false and holds?(f, id, header(s))
      end)

      expected = later + 2

      assert {:row, [^expected]} = replica_query(root, f, id, "SELECT count(*) FROM t"),
             "precondition: F's replica does not even hold the current generation"

      assert {:row, [1]} = replica_query(root, f, id, "SELECT count(*) FROM lost"),
             "F missed generation g entirely, was told its old generation was complete, absorbed " <>
               "it and cleared torn: an untorn, promotable replica missing a quorum-acked write"
    end
  end
end
