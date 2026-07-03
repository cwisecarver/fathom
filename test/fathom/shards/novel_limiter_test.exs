defmodule Fathom.Shards.NovelLimiterTest do
  # Finding #14, the churn half. :max_open_shards bounds how many shards a node holds
  # open; pre-fix nothing bounded how FAST a spray of novel valid Host ids could mint
  # new ones — each request cost a coordinator + fds + file + S3 lock PUT + Postgres row
  # all the way to the cap. Invariant pinned: with :novel_shard_rate set, an over-budget
  # NOVEL creation is refused ({:error, :novel_shard_rate_limited} → 429) before any of
  # that work runs, while existing shards (directory row, local file, or running
  # coordinator) are never limited.
  #
  # DataCase (not async): the Shards gate reads the directory in the CALLER, so the
  # sandbox connection this test owns is the one the gate sees; app env is mutated.
  use Fathom.DataCase, async: false

  alias Fathom.Shards.NovelLimiter
  alias Fathom.{Directory, Shards}

  setup do
    prev_rate = Application.get_env(:fathom, :novel_shard_rate)
    prev_burst = Application.get_env(:fathom, :novel_shard_burst)

    on_exit(fn ->
      restore_env(:novel_shard_rate, prev_rate)
      restore_env(:novel_shard_burst, prev_burst)
    end)

    :ok
  end

  defp restore_env(key, nil), do: Application.delete_env(:fathom, key)
  defp restore_env(key, value), do: Application.put_env(:fathom, key, value)

  defp unique_shard(prefix) do
    shard = "#{prefix}_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      local = Path.join([System.tmp_dir!(), "fathom_shards", "#{shard}.db"])
      remote = Path.join([System.tmp_dir!(), "fathom_remote_test", "#{shard}.db"])
      for base <- [local, remote], suffix <- ["", "-wal", "-shm"], do: File.rm(base <> suffix)
    end)

    shard
  end

  # Checks in immediately: these tests are about admission, not held connections.
  defp checkout(shard) do
    case Shards.checkout(shard) do
      {:ok, pid, ref, _path} ->
        Fathom.Shard.checkin(pid, ref)
        :ok

      {:error, _} = error ->
        error
    end
  end

  # Expert review #28: novel_refused?/1 called NovelLimiter.allow with no exit
  # protection, while the adjacent directory read carefully caught :exit. The limiter's
  # own backpressure model is mailbox saturation — exactly when GenServer.call starts
  # exiting :timeout — and a crash/restart window exits :noproc. Un-caught, the spray
  # the limiter exists to absorb crashed the whole open path (stream 500s) instead of
  # refusing cleanly. The invariant: a down/saturated limiter fails CLOSED to the same
  # clean {:error, :novel_shard_rate_limited} refusal.
  test "a down limiter fails closed to a clean rate-limit refusal, not a caller crash" do
    Application.put_env(:fathom, :novel_shard_rate, 1000)
    shard = unique_shard("novel_down")

    # Take the app's limiter down without a restart (a supervisor terminate_child
    # stays down until restart_child) — the :noproc window, held open deterministically.
    :ok = Supervisor.terminate_child(Fathom.DataPlane.Supervisor, Fathom.Shards.NovelLimiter)

    on_exit(fn ->
      Supervisor.restart_child(Fathom.DataPlane.Supervisor, Fathom.Shards.NovelLimiter)
    end)

    assert {:error, :novel_shard_rate_limited} = checkout(shard),
           "an exit from the limiter must fail closed, not crash the open"

    {:ok, _} = Supervisor.restart_child(Fathom.DataPlane.Supervisor, Fathom.Shards.NovelLimiter)
  end

  # --- the token bucket itself (private instance, injected clock) ---

  describe "the bucket" do
    defp bucket(clock) do
      name = :"limiter_#{System.unique_integer([:positive])}"
      now_fun = fn -> Agent.get(clock, & &1) end
      start_supervised!({NovelLimiter, name: name, now_fun: now_fun})
      name
    end

    test "grants the burst, then refuses until time refills" do
      Application.put_env(:fathom, :novel_shard_rate, 1)
      Application.put_env(:fathom, :novel_shard_burst, 2)
      clock = start_supervised!({Agent, fn -> 0 end})
      limiter = bucket(clock)

      assert :ok = NovelLimiter.allow("a", limiter)
      assert :ok = NovelLimiter.allow("b", limiter)
      assert {:error, :novel_shard_rate_limited} = NovelLimiter.allow("c", limiter)

      # 1 token/s: one second later exactly one more grant exists.
      Agent.update(clock, fn _ -> 1_000 end)
      assert :ok = NovelLimiter.allow("d", limiter)
      assert {:error, :novel_shard_rate_limited} = NovelLimiter.allow("e", limiter)
    end

    test "refill is capped at the burst budget" do
      Application.put_env(:fathom, :novel_shard_rate, 1)
      Application.put_env(:fathom, :novel_shard_burst, 2)
      clock = start_supervised!({Agent, fn -> 0 end})
      limiter = bucket(clock)

      # An hour idle refills to the burst (2), not 3600 tokens.
      Agent.update(clock, fn _ -> 3_600_000 end)
      assert :ok = NovelLimiter.allow("a", limiter)
      assert :ok = NovelLimiter.allow("b", limiter)
      assert {:error, :novel_shard_rate_limited} = NovelLimiter.allow("c", limiter)
    end
  end

  # --- the Shards admission gate (the app-global limiter) ---

  describe "the checkout gate" do
    defp exhaust_bucket! do
      # Burst 1 (reset, since the node-global bucket carries state across tests) + a
      # consumed token = an empty bucket for the next novel id (refill at 1/s is < 1
      # token within this test's runtime).
      Application.put_env(:fathom, :novel_shard_rate, 1)
      Application.put_env(:fathom, :novel_shard_burst, 1)
      NovelLimiter.reset()
      sacrifice = unique_shard("novel_grant")
      assert :ok = checkout(sacrifice)
      sacrifice
    end

    test "an over-rate novel creation is refused before any work runs" do
      exhaust_bucket!()
      refused = unique_shard("novel_refused")

      # Pre-fix this opened fine — the spray minted shards at line rate to the cap.
      assert {:error, :novel_shard_rate_limited} = checkout(refused)

      # Refused BEFORE creation: no coordinator, no local file.
      assert Registry.lookup(Fathom.ShardRegistry, refused) == []
      refute File.exists?(Fathom.Shard.db_path(refused))
    end

    test "a directory-known shard is never limited" do
      known = unique_shard("novel_known")
      {:ok, _} = Directory.resolve(known)
      exhaust_bucket!()

      assert :ok = checkout(known)
    end

    test "a shard with a local file is never limited" do
      warm = unique_shard("novel_warm")
      File.mkdir_p!(Path.dirname(Fathom.Shard.db_path(warm)))
      File.touch!(Fathom.Shard.db_path(warm))
      exhaust_bucket!()

      assert :ok = checkout(warm)
    end

    test "an already-running coordinator is never limited (registry-hit path)" do
      granted = exhaust_bucket!()
      # Same shard again with an empty bucket: the registry hit skips the gate.
      assert :ok = checkout(granted)
    end

    test "unset rate (the default) means the gate is off" do
      Application.delete_env(:fathom, :novel_shard_rate)
      assert :ok = checkout(unique_shard("novel_off"))
    end

    test "the executor surfaces the refusal as 429" do
      exhaust_bucket!()
      refused = unique_shard("novel_http")

      assert {:error, %Filo.Error{status: 429, code: "FILO_RATE_LIMITED"}} =
               Fathom.ShardExecutor.open(refused)
    end
  end
end
