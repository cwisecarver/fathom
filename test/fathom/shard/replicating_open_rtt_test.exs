defmodule Fathom.Shard.ReplicatingOpenRttTest do
  @moduledoc """
  Serial storage round trips on a REPLICATING open (perf review 2026-10-14 #3).

  With replication on (the prod default) an open reads the object's head under the lease: for the
  lineage it stamps, for promote-on-open's decision, and — on a warm open — for the post-lease
  authority check. That HEAD used to be issued only after the pull was awaited, and a warm open paid
  a second HEAD of the same object for the check. `Fathom.Test.LatencyStorage` makes each call on one
  shard cost a fixed round trip and records start/finish, so the structure of the open is visible.

  Measured at D = 100 ms per call, pull = 2 D (a GET that transfers a body), replication on:

  |            | before | after |
  |------------|--------|-------|
  | cold open  | ~300   | ~200  | (pull overlaps acquire; HEAD now overlaps the pull's tail)
  | warm open  | ~300   | ~200  | (fork HEAD overlaps acquire; ONE HEAD replaces check + lineage)

  The cold saving is `min(HEAD, pull - acquire)`: with a pull no longer than the lock PUT the HEAD
  (which must follow the lease, see `Shard.start_open_head/2`) still ends the open, so a tiny shard's
  cold open stays at 2 round trips. That is why the structural assertions (HEAD starts after the
  lease finished, and before the pull finished) are the load-bearing ones and the wall-clock ones
  are `@tag :bench` ceilings.

  Both liveness modes, `acquire_gen` asserted (the lease paths differ).

  The "pull fails" test does NOT discriminate the abandon path by itself: the head task is linked, so
  the coordinator's exit would kill it anyway. It is an invariant guard (no leaked process, no
  orphaned pull temp), and `abandon_open_head/1` is what makes it prompt.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Fathom.Shard
  alias Fathom.Shard.{Connection, Heartbeat, Provenance, Storage}
  alias Fathom.Shard.Storage.Local
  alias Fathom.Shards

  @modes [:legacy, :heartbeat]
  @d 100

  @env [
    :shard_storage,
    :latency_storage,
    :replication_enabled,
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :shard_idle_ms,
    :shard_flush_interval_ms
  ]

  setup do
    shard = "rtt_#{System.unique_integer([:positive])}"
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      Shards.drain(shard, 2_000)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}*")), do: File.rm_rf(f)
      Local.purge_shard(shard)
    end)

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_promote_on_open, true)
    Application.put_env(:fathom, :replication_recover_from_peers, false)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :shard_flush_interval_ms, 0)

    %{shard: shard}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    :ok
  end

  defp latency!(shard, extra \\ %{}) do
    Application.put_env(:fathom, :latency_storage,
      shard: shard,
      delay_ms: @d,
      sink: self(),
      extra: extra
    )

    Application.put_env(:fathom, :shard_storage, Fathom.Test.LatencyStorage)
  end

  defp db_bytes do
    path = Path.join(System.tmp_dir!(), "rtt_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(path)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('x')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(path)
    for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    bytes
  end

  # Put the shard's object in the "remote" directly through Local (no latency, no sink traffic).
  defp seed_object!(shard) do
    tmp = Path.join(System.tmp_dir!(), "rtt_seed_#{System.unique_integer([:positive])}.db")
    File.write!(tmp, db_bytes())
    assert {:ok, etag, _lineage} = Local.flush(shard, tmp, nil, nil, nil, [])
    File.rm(tmp)
    etag
  end

  defp place_warm!(shard, etag) do
    path = Shard.db_path(shard)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, db_bytes())
    Provenance.write(path, etag)
  end

  # Open the shard and answer {elapsed_ms, coordinator pid, [{op, started, finished}]}.
  defp timed_open!(shard) do
    flush_calls()
    t0 = System.monotonic_time(:millisecond)
    {:ok, pid} = Shards.ensure(shard)
    {:ok, _ref, _path} = Shard.checkout(pid)
    elapsed = System.monotonic_time(:millisecond) - t0
    {elapsed, pid, drain_calls([])}
  end

  defp flush_calls do
    receive do
      {:storage_call, _, _, _} -> flush_calls()
    after
      0 -> :ok
    end
  end

  defp drain_calls(acc) do
    receive do
      {:storage_call, op, s, f} -> drain_calls([{op, s, f} | acc])
    after
      0 -> Enum.sort_by(acc, fn {_, s, _} -> s end)
    end
  end

  defp call!(calls, op), do: Enum.find(calls, fn {o, _, _} -> o == op end)
  defp count(calls, op), do: Enum.count(calls, fn {o, _, _} -> o == op end)

  for mode <- @modes do
    describe "#{mode} mode" do
      setup %{shard: shard} do
        set_mode!(unquote(mode))
        latency!(shard, %{pull: @d})
        :ok
      end

      test "cold: the head starts after the lease and overlaps the pull", %{shard: shard} do
        seed_object!(shard)
        {_elapsed, pid, calls} = timed_open!(shard)

        assert :sys.get_state(pid).acquire_gen != nil == (unquote(mode) == :heartbeat),
               "the liveness mode did not take"

        {_, _, lease_done} = call!(calls, :acquire_lease)
        {_, head_start, _head_done} = call!(calls, :object_head)
        {_, _, pull_done} = call!(calls, :pull)

        assert count(calls, :object_head) == 1
        # The lease-held invariant: the HEAD describes the object AFTER we own the lock.
        assert head_start >= lease_done,
               "the head was issued before the lease finished (stale head, parked as unsafe)"

        # The point: it no longer waits for the pull to finish. This ORDERING is what discriminates
        # (pre-fix the head starts only after the pull is done) and it does not depend on machine
        # load. The wall-clock bound that used to sit here (elapsed < 2.5D) failed at 313 ms under a
        # loaded precommit run (2026-10-14) without anything being serial, so it is gone; the
        # critical-path claim (max(pull 2D, acquire D + head D) = 2D, not 3D) follows from the order.
        assert head_start < pull_done, "the head still waits for the whole pull"
      end

      test "cold: the lineage is the one a serial head would give", %{shard: shard} do
        seed_object!(shard)
        {:ok, head} = Local.object_head(shard)
        {_elapsed, pid, _calls} = timed_open!(shard)

        assert :sys.get_state(pid).lineage == Storage.next_lineage(head),
               "the overlapped head must yield the lineage the serial read did"
      end

      test "warm: one head serves the authority check and the lineage", %{shard: shard} do
        etag = seed_object!(shard)
        place_warm!(shard, etag)
        {elapsed, pid, calls} = timed_open!(shard)

        assert :sys.get_state(pid).acquire_gen != nil == (unquote(mode) == :heartbeat)
        refute call!(calls, :pull), "a matching warm copy must not pull"

        # fork-evidence HEAD (object_etag) overlaps the acquire; ONE object_head follows the lease.
        assert count(calls, :object_etag) == 1, inspect(calls)
        assert count(calls, :object_head) == 1, inspect(calls)

        {_, _, lease_done} = call!(calls, :acquire_lease)
        {_, head_start, _} = call!(calls, :object_head)
        assert head_start >= lease_done

        assert elapsed < 2 * @d + div(@d, 2) + 1, "warm open took #{elapsed} ms (3D = #{3 * @d})"
      end

      test "warm: a moved object is still quarantined and re-pulled from the shared head",
           %{shard: shard} do
        etag = seed_object!(shard)
        place_warm!(shard, "not-the-object-etag")
        {_elapsed, _pid, calls} = timed_open!(shard)

        # Fork evidence sees the mismatch before the lease and re-pulls cold: the same verdict as
        # without the shared head (the warm check never runs for a cold-resolved open).
        assert call!(calls, :pull)
        assert is_binary(etag)
      end

      test "a pull failure leaves no head task and no pull temp", %{shard: shard} do
        seed_object!(shard)

        # The head is slow and the pull fails at once, so the head is still in flight when the open
        # aborts.
        Application.put_env(:fathom, :latency_storage,
          shard: shard,
          delay_ms: 10,
          sink: self(),
          extra: %{object_head: 5_000},
          fail: %{pull: :boom}
        )

        {:ok, pid} = Shards.ensure(shard)
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, {:pull_failed, :boom}}}, 5_000

        assert_receive {:storage_object_head_started, head_pid}, 1_000
        head_ref = Process.monitor(head_pid)
        assert_receive {:DOWN, ^head_ref, :process, ^head_pid, _}, 1_000

        assert Path.wildcard(Shard.db_path(shard) <> ".pull*") == []
        refute_receive {:storage_call, :object_head, _, _}, 100
      end
    end
  end

  @tag :bench
  test "bench: warm replicating open stays within 2 round trips (was 3)", %{shard: shard} do
    set_mode!(:legacy)
    latency!(shard)
    etag = seed_object!(shard)
    place_warm!(shard, etag)
    {elapsed, _pid, _calls} = timed_open!(shard)
    assert elapsed < 2 * @d + div(@d, 2), "warm open #{elapsed} ms vs ceiling #{2 * @d + @d / 2}"
  end
end
