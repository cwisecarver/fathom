defmodule Fathom.Shard.ReplicationBlindIndexTest do
  @moduledoc """
  Expert review 2026-11-07 #10: a tenant commit whose WAL read hits an UNREADABLE wal-index was acked
  `:ok` without its frames being shipped, and the pinned checkpoint / idle close then treated them as
  quorum-covered.

  ## The defect

  With the `-shm` fd held and the cursor's salts unchanged, an unusable index (three torn reads, a
  short or zeroed `-shm`) makes `Wal.read_held/2` report the cursor's OLD extent `E_old` — "an older
  extent is a safe under-claim" (`Wal.walk_or_hold/6`). True for what to ship next; false for
  whether THIS caller's commit is covered. Every follower already held `E_old`, so `Primary.plan/3`
  planned `:nothing`, `ship_and_settle/5` took its `{:nothing, state} -> ack_if_owned` arm and
  replied `:ok` for frames `(E_old, E_new]` that were never read, never shipped and that no replica
  holds. `covered?/2` against the same header made a PASSIVE checkpoint backfill them too.

  ## The fixture

  `wal_index_test.exs`'s `blind_shm!/2` trick, through a real `Session` with three real loopback
  followers that all hold `E_old`: the Session's held `-shm` fd is swapped (inside the Session
  process, which owns the raw fd) for an empty file, which `Wal.read_index/1` reports as unusable.
  A second INSERT then lands frames past the cursor. Every precondition is asserted: the handle holds
  an `-shm` fd and a cursor, the WAL really grew, and no follower holds the new bytes.

  Both liveness modes run (the bug is before `ack_if_owned/1`, but the ack path differs).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Heartbeat
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shards
  import Fathom.Test.ReplicationCase

  @modes [:legacy, :heartbeat]

  setup do
    id = "repl_blind_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replblind_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    on_exit(fn ->
      Session.stop(id)
      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    owner = "n_#{System.unique_integer([:positive])}@test"
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000, owner: owner})
    _ = :sys.get_state(hb)
    :ok
  end

  defp assert_mode!(:legacy, coordinator),
    do: assert(is_nil(Fathom.Shard.acquire_gen(coordinator)), "expected LEGACY liveness")

  defp assert_mode!(:heartbeat, coordinator),
    do: refute(is_nil(Fathom.Shard.acquire_gen(coordinator)), "expected HEARTBEAT liveness")

  defp holders(followers, id, expected) do
    Enum.count(followers, fn {name, _} ->
      File.read(Follower.wal_path(name, id)) == {:ok, expected}
    end)
  end

  # A seeded 3-follower quorum, one acked commit (so the Session holds a WAL handle with a cursor and
  # every follower sits at `E_old`), then a SECOND insert whose frames are past that cursor.
  defp primed!(%{id: id, root: root}, mode) do
    set_mode!(mode)
    followers = start_followers!(root, 3, "bl")
    enable_replication!(followers, connected: :all)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    assert_mode!(mode, coordinator)

    {:ok, _} = Connection.query(conn, "CREATE TABLE t (a INTEGER, b TEXT)", [])
    {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1, 'one')", [])

    wal = path <> "-wal"
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)
    assert :ok = Session.commit(id, wal, coordinator)

    old = File.read!(wal)
    assert holders(followers, id, old) >= 2, "precondition: a quorum holds E_old"
    for {name, _} <- followers, do: await_wal!(name, id, old)

    session = session!(id)
    %{wal_fd: handle} = :sys.get_state(session)

    assert is_map(handle) and handle.shm != nil and is_map(handle.cursor),
           "precondition: the Session holds no -shm fd / cursor, so the blind fixture reaches nothing"

    %{
      id: id,
      followers: followers,
      coordinator: coordinator,
      conn: conn,
      wal: wal,
      old: old,
      session: session,
      root: root
    }
  end

  defp await_wal!(name, id, expected) do
    Fathom.Test.Await.until(
      fn -> File.read(Follower.wal_path(name, id)) == {:ok, expected} end,
      "follower #{name} never converged on the primary's WAL"
    )
  end

  # Swap the held -shm fd for an empty file INSIDE the Session (raw fds belong to the process that
  # opened them). Returns the real fd so the test can put it back.
  defp blind!(session, root) do
    empty = Path.join(root, "empty-shm-#{System.unique_integer([:positive])}")
    File.write!(empty, <<>>)
    test = self()

    :sys.replace_state(session, fn s ->
      {:ok, fd} = :file.open(empty, [:read, :raw, :binary])
      send(test, {:real_shm, s.wal_fd.shm})
      %{s | wal_fd: %{s.wal_fd | shm: fd}}
    end)

    assert_receive {:real_shm, real}
    real
  end

  defp unblind!(session, real) do
    :sys.replace_state(session, fn s ->
      :ok = :file.close(s.wal_fd.shm)
      %{s | wal_fd: %{s.wal_fd | shm: real}}
    end)
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      test "a commit whose frames the blind index hid is NOT acked until they reach a quorum",
           ctx do
        %{
          id: id,
          followers: followers,
          coordinator: coordinator,
          conn: conn,
          wal: wal,
          old: old,
          session: session,
          root: root
        } = primed!(ctx, unquote(mode))

        real = blind!(session, root)
        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2, 'two')", [])
        grown = File.read!(wal)

        assert byte_size(grown) > byte_size(old),
               "precondition: the second commit wrote no frames"

        assert {:error, :wal_index_unreadable} = Session.commit(id, wal, coordinator),
               "a commit whose frames were never read was acked (pre-fix: :ok with zero pushes)"

        # And the refusal is honest: nothing past E_old reached any follower.
        assert holders(followers, id, grown) == 0,
               "precondition: a follower holds the new frames, so the refusal proves nothing"

        # The index comes back: the retry ships the frames and only then acks.
        unblind!(session, real)
        assert :ok = Session.commit(id, wal, coordinator)
        assert holders(followers, id, grown) >= 2
      end

      test "idle-close does not call a blind WAL covered", ctx do
        %{id: id, conn: conn, wal: wal, old: old, session: session, root: root, followers: fs} =
          primed!(ctx, unquote(mode))

        real = blind!(session, root)
        {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2, 'two')", [])
        grown = File.read!(wal)

        assert byte_size(grown) > byte_size(old),
               "precondition: the second commit wrote no frames"

        assert holders(fs, id, grown) == 0

        token = make_ref()
        send(session, {:idle_close?, self(), token})
        assert_receive {:idle_close_verdict, ^token, verdict}, 5_000

        # `:ready` would let the coordinator's idle drain run the close-time checkpoint, which
        # backfills frames no follower holds and restarts the log over them.
        refute verdict == :ready,
               "idle-close judged a WAL covered from a header that could not see its frames"

        unblind!(session, real)
      end
    end
  end
end
