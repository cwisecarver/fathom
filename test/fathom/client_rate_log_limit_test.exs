defmodule Fathom.ClientRateLogLimitTest do
  @moduledoc """
  Expert review 2026-11-07 **#45, part 2**: lines logged once per client request (or once per
  replicated commit) in a failure storm flood the log and push out the useful lines, and with the
  logger dropping instead of blocking their cost is a dropped line, not a blocked caller. Each one
  now goes through `Fathom.LogLimiter` (per node, 5 per 1 s window per key, the first line of the
  next window carrying the suppressed count: `log_limiter_test.exs`).

  Each test fires a burst of identical failures and asserts a BOUNDED number of lines. The bound is
  10 for a burst of 40: five per window, and a burst that straddles a window boundary spends two.
  Every key starts from a fresh window (the limiter is global to the node, and other tests spend
  it). The revocation lines are pinned in `hrana_auth_cold_floor_test.exs`.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.LogLimiter
  alias Fathom.Shard.Replication.{Follower, Protocol}
  alias Fathom.ShardExecutor
  alias Fathom.Test.Env

  @burst 40
  @bound 10

  defp fresh!(keys) do
    for k <- keys, do: LogLimiter.reset(k)
    on_exit(fn -> for k <- keys, do: LogLimiter.reset(k) end)
  end

  defp lines(log, needle), do: length(String.split(log, needle)) - 1

  describe "ShardExecutor.open_error/1" do
    setup do
      fresh!([:open_failed, :open_retryable, :open_unrecoverable])
    end

    for {name, reason, needle, code} <- [
          {"an unclassified open failure", {:some_unknown, :reason}, "cannot open shard:",
           "FILO_OPEN_FAILED"},
          {"a retryable open failure", :timeout, "shard open failed (retryable)",
           "FILO_UNAVAILABLE"},
          {"an open that needs an operator", {:pull_failed, :sidecar_corrupt},
           "needs an operator", "FILO_SHARD_UNRECOVERABLE"}
        ] do
      test "#{name}: every request still gets its error, the log gets a bounded number of lines" do
        log =
          capture_log(fn ->
            for _ <- 1..@burst do
              assert %{code: unquote(code)} =
                       ShardExecutor.open_error(unquote(Macro.escape(reason)))
            end
          end)

        n = lines(log, unquote(needle))
        assert n >= 1, "the incident was not logged at all"
        assert n <= @bound, "#{n} lines for #{@burst} identical open failures"
      end
    end
  end

  describe "follower push lines" do
    setup do
      fresh!([:follower_push_busy, :follower_seed_requested])

      # Both lines are `Logger.info`; the test env's level drops them before the limiter matters.
      previous = Logger.level()
      Logger.configure(level: :info)
      on_exit(fn -> Logger.configure(level: previous) end)

      name = :"crll_f#{System.unique_integer([:positive])}"
      dir = Path.join(System.tmp_dir!(), to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      on_exit(fn -> File.rm_rf(dir) end)

      # Floors any real volume clears, and a short lock wait so a busy replica refuses at once.
      Env.put!(:replication_disk_free_floor_bytes, 1)
      Env.put!(:replication_disk_free_floor_inodes, 1)
      Env.put!(:replication_shard_lock_wait_ms, 1)
      _ = :sys.get_state(pid)

      {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
      {:ok, port} = :inet.port(l)
      {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
      {:ok, server} = :gen_tcp.accept(l)
      :gen_tcp.close(l)

      worker = Follower.spawn_worker(server, name, nil, self(), :atomics.new(1, []))
      Process.unlink(worker)
      on_exit(fn -> Process.exit(worker, :kill) end)

      %{
        name: name,
        worker: worker,
        client: client,
        id: "crll_#{System.unique_integer([:positive])}"
      }
    end

    defp push(id, offset),
      do: %Protocol.Push{
        shard_id: id,
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        offset: offset,
        payload: "frames"
      }

    defp ask(%{worker: worker, client: client}, push) do
      send(worker, {:frame, push, 10})
      assert_receive {:frame_done, 10}, 5_000
      {:ok, reply} = :gen_tcp.recv(client, 0, 5_000)
      {:ok, decoded} = Protocol.decode(reply)
      decoded
    end

    # Pre-fix: one line per push, 40 for the burst.
    test "a replica held busy refuses a burst of pushes, logging a bounded number of lines",
         ctx do
      %{name: name, id: id} = ctx
      File.write!(Follower.db_path(name, id), "the existing db")
      File.write!(Follower.wal_path(name, id), "")
      :ok = Follower.seed(name, id, 1, 0, 0, 0)
      {:ok, lock} = Follower.lock_shard(name, id, 0)

      log =
        capture_log(fn ->
          for _ <- 1..@burst, do: assert({:reject, _, :internal, _} = ask(ctx, push(id, 0)))
        end)

      Follower.unlock_shard(name, lock)

      n = lines(log, "its replica is busy")
      assert n >= 1, "the incident was not logged at all"
      assert n <= @bound, "#{n} lines for #{@burst} refused pushes"
    end

    # Pre-fix: one `asking for a seed` line per push of a torn replica.
    test "a torn replica asking for a seed on every push logs a bounded number of lines", ctx do
      %{name: name, id: id} = ctx
      :ok = Follower.seed(name, id, 1, 0, 0, 4096)
      File.write!(Follower.db_path(name, id), "the replica's database")
      refute File.exists?(Follower.wal_path(name, id)), "fixture: the WAL must be missing"

      # The first push finds the WAL gone and marks the replica torn.
      assert {:reject, _, :unknown_shard, _} = ask(ctx, push(id, 4096))
      assert %{torn: true} = Follower.state_of(name, id)

      log =
        capture_log(fn ->
          for _ <- 1..@burst,
              do: assert({:reject, _, :unknown_shard, _} = ask(ctx, push(id, 4096)))
        end)

      n = lines(log, "asking for a seed")
      assert n >= 1, "the incident was not logged at all"
      assert n <= @bound, "#{n} lines for #{@burst} seed requests"
    end
  end
end
