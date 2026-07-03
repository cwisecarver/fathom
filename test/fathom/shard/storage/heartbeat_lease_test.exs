defmodule Fathom.Shard.Storage.HeartbeatLeaseTest do
  @moduledoc """
  Storage-level semantics for the node-heartbeat liveness model (the F1 fix): a
  shard's owner is live iff its per-node *heartbeat* is fresh — NOT the lock's own
  TTL. So `acquire_lease/3` consults the current owner's heartbeat to decide
  held-vs-steal, and fails closed when it can't read it. Exercised against the
  Local backend (the faithful double for the S3 fence).
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

    File.write!(
      Path.join([dir, "heartbeats", owner]),
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

  test "a heartbeat expired past the steal margin is stealable", %{
    shard: shard,
    dir: dir,
    a: a,
    b: b
  } do
    assert {:ok, %{epoch: 1}} = Local.acquire_lease(shard, a, @ttl)
    # a's heartbeat lapsed well PAST the clock-skew steal margin — genuinely dead.
    past = System.system_time(:millisecond) - Storage.steal_margin_ms() - 1_000
    write_heartbeat(dir, a, past)

    assert {:ok, %{owner: ^b, epoch: 2}} = Local.acquire_lease(shard, b, @ttl)
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
