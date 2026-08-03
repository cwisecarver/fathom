defmodule Fathom.Shard.Storage.HeartbeatLeaseTest do
  @moduledoc """
  Storage-level semantics for the node-heartbeat liveness model (the F1 fix): the per-node
  *heartbeat* is the PRIMARY liveness signal, so `acquire_lease/3` consults the current owner's
  heartbeat to decide held-vs-steal, and fails closed when it can't read it. Exercised against
  the Local backend (the faithful double for the S3 fence).

  This moduledoc used to say the owner is live iff the heartbeat is fresh, "NOT the lock's own
  TTL". That is no longer true and was the bug: an owner is dead only when **both** its heartbeat
  and its lock TTL have lapsed (expert review 2026-08-01 #12). A stale-but-present heartbeat was
  otherwise strictly WORSE than an absent one — the `:not_found` branch already fell back to the
  lock TTL — so one Heartbeat process crash, which deliberately leaves its object behind, made a
  healthy node's entire keyspace slice instantly stealable while it kept serving.

  #12 was fixed in the S3 backend and MISSED in Local; the tests below pinned the Local defect,
  which is how it survived. They now expire the lock alongside the heartbeat where they mean
  "genuinely dead", and the `#12` describe block pins the invariant itself.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Local

  @ttl 30_000

  setup do
    # Isolate each test in its own remote dir so locks/heartbeats never collide.
    dir = Path.join(System.tmp_dir!(), "fathom_hb_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:fathom, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, Fathom.Shard.Storage.Local, dir: dir)

    on_exit(fn ->
      File.rm_rf!(dir)

      if prev,
        do: Application.put_env(:fathom, Fathom.Shard.Storage.Local, prev),
        else: Application.delete_env(:fathom, Fathom.Shard.Storage.Local)
    end)

    %{dir: dir, shard: "s_#{System.unique_integer([:positive])}", a: "node_a", b: "node_b"}
  end

  defp write_heartbeat(dir, owner, expires_at_ms) do
    File.mkdir_p!(Path.join(dir, "heartbeats"))

    # Encode the owner to match the backend's heartbeat_path (round-2 #3): the backend
    # reads `heartbeats/<encoded-owner>`, so a raw-path write is invisible to it.
    File.write!(
      Path.join([dir, "heartbeats", URI.encode_www_form(owner)]),
      Storage.encode_heartbeat(%{owner: owner, expires_at_ms: expires_at_ms})
    )
  end

  # Write the shard's `.lock` object directly with a chosen expiry — to set up a legacy-mode
  # owner (no heartbeat) whose lock TTL is fresh vs expired.
  defp write_lock(dir, shard, owner, epoch, expires_at_ms) do
    File.mkdir_p!(dir)

    File.write!(
      Path.join(dir, "#{shard}.lock"),
      Storage.encode_lease(%{owner: owner, epoch: epoch, expires_at_ms: expires_at_ms})
    )
  end

  test "heartbeat renew/read/clear roundtrip", %{a: a} do
    assert Local.read_heartbeat(a) == :not_found
    assert {:ok, %{owner: ^a, expires_at_ms: exp}} = Local.renew_heartbeat(a, @ttl)
    assert {:ok, %{owner: ^a, expires_at_ms: ^exp}} = Local.read_heartbeat(a)
    assert :ok = Local.clear_heartbeat(a)
    assert Local.read_heartbeat(a) == :not_found
  end

  # Expert review #39: renew_heartbeat used a bare File.write (open-truncate-write), so a
  # concurrent read_heartbeat — another shard's acquire_lease/owner_live?, or a test reading
  # right as the supervisor-restarted heartbeat re-renews — could observe the empty/truncated
  # file between truncate and write: a spurious :corrupt_heartbeat that fails lease
  # acquisition closed (this bit CI on the #7 regression test). The invariant, same as the
  # data/lease objects: renewals swap the object in whole (temp + rename), so an fd opened
  # before a renewal still reads the complete OLD document, never a torn one.
  test "renew_heartbeat replaces the object whole, not in place", %{dir: dir, a: a} do
    assert {:ok, %{expires_at_ms: exp1}} = Local.renew_heartbeat(a, @ttl)

    # A concurrent reader: opened before the next renewal.
    {:ok, fd} = File.open(Path.join([dir, "heartbeats", a]), [:read, :binary])

    assert {:ok, %{expires_at_ms: exp2}} = Local.renew_heartbeat(a, @ttl * 2)
    assert exp2 > exp1

    # Rename semantics: the pre-renewal fd points at the old inode and reads the whole
    # OLD heartbeat. An in-place truncate+write would show the new bytes (or a torn
    # prefix) through this same fd.
    assert {:ok, %{owner: ^a, expires_at_ms: ^exp1}} =
             Storage.decode_heartbeat(IO.binread(fd, 10_000))

    File.close(fd)

    assert {:ok, %{expires_at_ms: ^exp2}} = Local.read_heartbeat(a)
  end

  test "a live owner (fresh heartbeat) blocks a steal", %{shard: shard, a: a, b: b} do
    {:ok, _} = Local.renew_heartbeat(a, @ttl)
    assert {:ok, %{owner: ^a, epoch: 1}} = Local.acquire_lease(shard, a, @ttl)

    # b cannot steal — a's heartbeat is fresh, regardless of the lock's own TTL.
    assert {:error, {:held, ^a}} = Local.acquire_lease(shard, b, @ttl)
  end

  test "an owner with no heartbeat AND an expired lock TTL is stealable (epoch bumps)",
       %{shard: shard, dir: dir, a: a, b: b} do
    {:ok, _} = Local.renew_heartbeat(a, @ttl)
    assert {:ok, %{epoch: 1}} = Local.acquire_lease(shard, a, @ttl)

    # a crashes: heartbeat gone AND it stopped renewing its lock (TTL lapsed past the margin) —
    # genuinely dead in legacy terms, so it's stealable via the lock-TTL fallback.
    :ok = Local.clear_heartbeat(a)
    stale = System.system_time(:millisecond) - Storage.steal_margin_ms() - 1_000
    write_lock(dir, shard, a, 1, stale)

    assert {:ok, %{owner: ^b, epoch: 2}} = Local.acquire_lease(shard, b, @ttl)
  end

  # Finding #11: with no heartbeat object at all (heartbeat_server: false legacy mode), the
  # steal decision must fall back to the lock's own TTL — otherwise a live owner that renews
  # its lock per-shard looks instantly dead and any contender steals it. A fresh lock TTL
  # protects the owner even with no heartbeat.
  test "a legacy owner (no heartbeat) with a fresh lock TTL is NOT stolen",
       %{shard: shard, dir: dir, a: a, b: b} do
    fresh = System.system_time(:millisecond) + @ttl
    write_lock(dir, shard, a, 1, fresh)
    # No heartbeat object for a exists.
    assert Local.read_heartbeat(a) == :not_found

    assert {:error, {:held, ^a}} = Local.acquire_lease(shard, b, @ttl)
    # No steal happened — a still holds {a, epoch 1}.
    assert Local.check_lease(shard, %{owner: a, epoch: 1}) == :ok
  end

  test "a heartbeat AND lock TTL both expired past the steal margin is stealable", %{
    shard: shard,
    dir: dir,
    a: a,
    b: b
  } do
    assert {:ok, %{epoch: 1}} = Local.acquire_lease(shard, a, @ttl)
    # a is genuinely dead: BOTH signals lapsed well past the clock-skew steal margin.
    #
    # This test previously expired only the heartbeat and still asserted a steal — it pinned the
    # pre-#12 Local behaviour, where a lapsed heartbeat alone was enough. Expiring the lock too
    # is what "genuinely dead" means now; the #12 block below pins the half that must NOT steal.
    past = System.system_time(:millisecond) - Storage.steal_margin_ms() - 1_000
    write_heartbeat(dir, a, past)
    write_lock(dir, shard, a, 1, past)

    assert {:ok, %{owner: ^b, epoch: 2}} = Local.acquire_lease(shard, b, @ttl)
  end

  describe "#12 — a lapsed heartbeat alone is not death" do
    test "stale heartbeat + fresh lock TTL is HELD, not stolen", %{
      shard: shard,
      dir: dir,
      a: a,
      b: b
    } do
      # The reachable-by-design case: the Heartbeat GenServer died abnormally and deliberately
      # left its object behind, while the node is healthy and renewing every lock it holds.
      past = System.system_time(:millisecond) - Storage.steal_margin_ms() - 1_000
      write_heartbeat(dir, a, past)
      write_lock(dir, shard, a, 3, System.system_time(:millisecond) + @ttl)

      assert {:error, {:held, ^a}} = Local.acquire_lease(shard, b, @ttl),
             "a stale heartbeat alone made a lock-renewing owner stealable — one process crash " <>
               "would hand away that node's entire keyspace slice while it kept serving"

      # And nothing moved: a still holds {a, 3}.
      assert Local.check_lease(shard, %{owner: a, epoch: 3}) == :ok
    end

    test "a stale-but-present heartbeat is no worse than an absent one", %{
      shard: shard,
      dir: dir,
      a: a,
      b: b
    } do
      # The asymmetry that made #12 a bug: the `:not_found` branch already fell back to the lock
      # TTL, so a node was SAFER having no heartbeat object than a stale one. Pin that the two
      # now agree.
      fresh = System.system_time(:millisecond) + @ttl
      write_lock(dir, shard, a, 1, fresh)

      assert Local.read_heartbeat(a) == :not_found
      assert {:error, {:held, ^a}} = Local.acquire_lease(shard, b, @ttl)

      write_heartbeat(
        dir,
        a,
        System.system_time(:millisecond) - Storage.steal_margin_ms() - 1_000
      )

      assert {:error, {:held, ^a}} = Local.acquire_lease(shard, b, @ttl),
             "adding a stale heartbeat made a protected owner stealable"
    end
  end

  test "a heartbeat expired WITHIN the steal margin is not stolen (clock-skew guard)",
       %{shard: shard, dir: dir, a: a, b: b} do
    assert {:ok, %{epoch: 1}} = Local.acquire_lease(shard, a, @ttl)
    # a's heartbeat is expired, but by less than the steal margin: a peer whose clock
    # merely runs ahead must NOT steal a still-live owner. This is the invariant that
    # keeps a transient NTP skew from opening a double-write window.
    recent = System.system_time(:millisecond) - div(Storage.steal_margin_ms(), 2)
    write_heartbeat(dir, a, recent)

    assert {:error, {:held, ^a}} = Local.acquire_lease(shard, b, @ttl)
  end

  # Expert review #6: owner identity was the bare node name, so a NEW incarnation of
  # the same node (replaced pod, blue/green overlap) hit the same-owner reclaim path
  # and got a lease BYTE-IDENTICAL to the old incarnation's ({owner, epoch}) — both
  # zombies passed every fence simultaneously, and the epoch stopped being a fencing
  # token. With incarnation-qualified owners, a later boot is a FOREIGN owner: a live
  # zombie's lock is protected by its liveness, and taking over a dead one BUMPS the
  # epoch.
  test "a later incarnation cannot silently reclaim the previous one's lock",
       %{shard: shard, dir: dir} do
    inc1 = "samenode@host#aaaa1111"
    inc2 = "samenode@host#bbbb2222"

    # The previous incarnation holds the lock and is still alive (a lingering zombie).
    {:ok, _} = Local.renew_heartbeat(inc1, @ttl)
    write_lock(dir, shard, inc1, 3, System.system_time(:millisecond) + @ttl)

    # The new boot is a foreign owner — the live zombie's lock is NOT reclaimable.
    assert {:error, {:held, ^inc1}} = Local.acquire_lease(shard, inc2, @ttl)

    # The zombie dies — BOTH its heartbeat and its lock TTL lapse (#12; this previously staled
    # only the heartbeat, pinning the pre-#12 Local behaviour). The point of this test is the
    # EPOCH BUMP, which is unchanged: the new incarnation steals at 4, never a silent
    # same-epoch reclaim at 3.
    past = System.system_time(:millisecond) - Storage.steal_margin_ms() - 1_000
    write_heartbeat(dir, inc1, past)
    write_lock(dir, shard, inc1, 3, past)

    assert {:ok, %{owner: ^inc2, epoch: 4}} = Local.acquire_lease(shard, inc2, @ttl)
  end

  test "the same incarnation still reclaims its own lock without an epoch bump",
       %{shard: shard, dir: dir} do
    inc = "samenode@host#cccc3333"
    {:ok, _} = Local.renew_heartbeat(inc, @ttl)
    write_lock(dir, shard, inc, 7, System.system_time(:millisecond) - 1)

    # Same boot re-acquiring (e.g. a coordinator restart within one VM): same writer,
    # same epoch — no fence discontinuity.
    assert {:ok, %{owner: ^inc, epoch: 7}} = Local.acquire_lease(shard, inc, @ttl)
  end

  test "check_lease confirms ownership and detects supersession",
       %{shard: shard, dir: dir, a: a, b: b} do
    {:ok, _} = Local.renew_heartbeat(a, @ttl)
    assert {:ok, lease_a} = Local.acquire_lease(shard, a, @ttl)
    assert Local.check_lease(shard, lease_a) == :ok

    # a lapses (heartbeat gone AND lock TTL expired) and b steals; a's old lease no longer
    # checks out.
    :ok = Local.clear_heartbeat(a)
    stale = System.system_time(:millisecond) - Storage.steal_margin_ms() - 1_000
    write_lock(dir, shard, a, lease_a.epoch, stale)

    {:ok, _} = Local.acquire_lease(shard, b, @ttl)
    assert Local.check_lease(shard, lease_a) == {:error, :superseded}
  end

  test "fail-closed: a steal is refused when the owner's heartbeat is unreadable",
       %{shard: shard, a: a, b: b} do
    {:ok, _} = Local.renew_heartbeat(a, @ttl)
    {:ok, %{epoch: 1}} = Local.acquire_lease(shard, a, @ttl)

    # b's node is partitioned from the store and can't read a's heartbeat. It must
    # NOT steal on uncertainty (that would create two owners) — it fails closed.
    prev = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :storage_fault, :read_heartbeat)

    on_exit(fn ->
      Application.delete_env(:fathom, :storage_fault)

      if prev,
        do: Application.put_env(:fathom, :shard_storage, prev),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    assert {:error, {:transient_lookup, _}} = Storage.acquire_lease(shard, b, @ttl)
    # No steal happened: a still holds {a, epoch 1} (a steal would have bumped it).
    assert Storage.check_lease(shard, %{owner: a, epoch: 1}) == :ok
  end
end
