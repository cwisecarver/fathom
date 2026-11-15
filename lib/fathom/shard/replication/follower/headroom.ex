defmodule Fathom.Shard.Replication.Follower.Headroom do
  @moduledoc """
  The Follower's replica-volume back-pressure: whether a NEW seed may be admitted, from the cached free
  space of the replica directory minus what in-flight seeds have already reserved (cleanliness audit
  2026-11-13 #33). Moved out of `Fathom.Shard.Replication.Follower`, whose row state machine it never
  touches.

  Everything here works on the Follower's `locks` ETS table and its replica `dir`, passed in, as
  tuple-keyed rows (`{:disk, dir}`, `{:disk_refresh, dir}`, `{:reserved, shard, pid}`,
  `{:seed_refused, shard}`): no per-shard lock and no row state. The decisions stay in the
  Follower: `refuse_seed/4` (what a refusal says and counts) and `checked_begin/5` (the in-lock checks)
  call into this module; this module never calls back.

    * `admit?/3`: reserve the incoming seed's bytes, admit only if free space minus every live
      reservation stays above the floors (bytes and inodes), else release and refuse. An unknown free
      space FAILS OPEN.
    * `below_floor?/2`, `seed_refusal_remembered?/2`, `remember_seed_refusal/2`,
      `prune_seed_refusals/1`: the early-refusal path for a begin of a shard that holds nothing.
    * The free-space cache: readers never fork `df` (the node poller fills it, or one background
      refresh does; see the comments on `disk_free/2`).
  """

  require Logger

  alias Fathom.Shard.Replication.Protocol

  # DISK BACK-PRESSURE ON THE REPLICA STORE (expert review 2026-08-20 #23).
  #
  # A node acting as a follower stores a full `.db` + `-wal` copy of EVERY shard it follows, and
  # this store had no bound of any kind: no count cap, no byte cap, no free-space floor, no
  # retention. `forget/2` is called from promotions and from tombstoning (a deleted tenant's replica
  # is erased, expert review 2026-09-29 #5) — never for a live shard. It grows monotonically
  # from OTHER NODES' write traffic, and both directories default under `System.tmp_dir!()`, so out
  # of the box the replica store and the live shard data dir SHARE A VOLUME.
  #
  # The asymmetry with the warm cache (removed 2026-09-14 with the WarmFollower) was the tell. That cache
  # held a READ copy whose loss cost only failover latency, and it had three independent bounds plus a
  # disk_pressure event. This store holds acked, quorum-durable writes and had a gauge and an alert rule.
  #
  # What a full volume does here is the unbounded-RPO failure AGENTS.md already documents: every
  # cold-open `pull` and every dirty shard's `VACUUM INTO` fails, while SQLite's own small WAL
  # appends keep succeeding — so this node's OWN tenants keep being ACKED writes that can never be
  # made durable, caused by its role as somebody else's follower, with no local signal.
  #
  # Refusing a NEW seed is the half that is unambiguous: a shard we do not yet hold is one whose
  # RPO simply stays at the stored object, which is the pre-A2 behaviour and always correct. What
  # to SHED once already full is a policy question (which replica, and what it does to the quorum's
  # effective redundancy) and is deliberately not decided here.
  #
  # RESERVED BYTES (expert review 2026-10-12 #18). The check used to read the volume's free space
  # and subtract only THIS seed's size, so N seeds beginning together each saw the same free space
  # and each passed — together they could blow through the floor. Each seed now RESERVES its
  # incoming bytes before checking and the check subtracts every live reservation, so concurrent
  # seeds see each other. Reserve-then-check (not check-then-reserve) so a race errs toward
  # refusing: two simultaneous begins may both refuse, never both pass. A refused seed is simply
  # re-seeded later, which is the documented cost of back-pressure.
  #
  # The reservation is released at install (success or failure), at discard, and when the begin is
  # refused. A crashed worker never gets to release, so a reservation whose owner is dead is
  # pruned on the next read rather than leaking free space forever. Bytes a seed has already
  # written are counted twice (reservation AND the lower free space) — conservative by design.
  #
  # The volume read is cached and refreshed off this path; see `disk_free/1`.
  def admit?(tab, dir, %Protocol.SeedBegin{} = b) do
    incoming = (b.db_size || 0) + (b.wal_size || 0)
    reserve(tab, b.shard_id, incoming)

    ok? = headroom?(tab, dir)

    unless ok?, do: release_reservation(tab, b.shard_id)
    ok?
  end

  # Whether a seed of `incoming` bytes would clear the floor right now, from the cached reading and
  # every live reservation (see `reserve_headroom?/2`).
  defp headroom?(tab, dir) do
    case disk_free(tab, dir) do
      # Cannot read the volume — fail OPEN (as the removed WarmFollower did). Refusing to
      # replicate because a stat failed would turn an observability gap into a durability one.
      :error ->
        true

      {:ok, free, free_inodes} ->
        free - reserved_bytes(tab) >= disk_free_floor_bytes() and inodes_ok?(free_inodes)
    end
  end

  def below_floor?(tab, dir) do
    case disk_free(tab, dir) do
      :error -> false
      {:ok, free, free_inodes} -> free < disk_free_floor_bytes() or not inodes_ok?(free_inodes)
    end
  end

  @default_seed_refusal_ms 30_000

  def remember_seed_refusal(tab, shard_id) do
    until = System.monotonic_time(:millisecond) + seed_refusal_ms()
    :ets.insert(tab, {{:seed_refused, shard_id}, until})
  rescue
    ArgumentError -> :ok
  end

  def seed_refusal_remembered?(tab, shard_id) do
    case :ets.lookup(tab, {:seed_refused, shard_id}) do
      [{key, until}] ->
        if until > System.monotonic_time(:millisecond) do
          true
        else
          :ets.delete_object(tab, {key, until})
          false
        end

      [] ->
        false
    end
  rescue
    ArgumentError -> false
  end

  # Expired memories of shards that never pushed again (a lookup deletes only what it reads).
  def prune_seed_refusals(tab) do
    now = System.monotonic_time(:millisecond)

    :ets.select_delete(tab, [
      {{{:seed_refused, :_}, :"$1"}, [{:"=<", :"$1", now}], [true]}
    ])
  rescue
    ArgumentError -> 0
  end

  defp seed_refusal_ms,
    do: Application.get_env(:fathom, :replication_seed_refusal_ms, @default_seed_refusal_ms)

  # Reservation rows live in the lock table under `{:reserved, shard_id, owner}` keys (a tuple, so
  # they can never collide with a binary shard id's lock row); the disk-info cache under
  # `{:disk, dir}` and its refresh claim under `{:disk_refresh, dir}`. The OWNER is in the key
  # (expert review 2026-10-22 r8 #10) because the reservation is now taken before the shard lock:
  # keyed by shard alone, a second connection's begin for the same shard would overwrite the
  # first's row, and its release would then delete it.
  defp reserve(tab, shard_id, bytes) do
    me = self()
    :ets.insert(tab, {{:reserved, shard_id, me}, me, bytes})
  rescue
    ArgumentError -> :ok
  end

  @doc false
  @spec release_reservation(:ets.table(), String.t()) :: :ok
  def release_reservation(tab, shard_id) do
    :ets.delete(tab, {:reserved, shard_id, self()})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # Sum of live reservations; a row whose owner process died (crash path) is dropped on the way.
  defp reserved_bytes(tab) do
    :ets.foldl(
      fn
        {{:reserved, _, _}, pid, bytes} = row, acc ->
          if Process.alive?(pid) do
            acc + bytes
          else
            :ets.delete_object(tab, row)
            acc
          end

        _other, acc ->
          acc
      end,
      0,
      tab
    )
  rescue
    ArgumentError -> 0
  end

  @disk_cache_ms 1_000

  # READERS NEVER FORK (expert review 2026-10-22 r8 #10, decision Q10 = A). The reading is cached
  # here, filled three ways: the node poller's 10 s `Measurements.disk/0` (`note_disk_reading/3`,
  # one `df` for the gauge and this check), a refresh started at boot, and a background refresh
  # when a reader finds it older than `@disk_cache_ms`. That refresh is SINGLE-FLIGHT and the reader
  # uses the value it already has, so a seed storm costs one `df` per second per volume instead of
  # one (plus a queued `disksup` call) per seed begin, and no begin waits on a subprocess.
  # Live reservations are still subtracted on every check, so concurrent seeds still see each
  # other; what a stale reading misses is only the last second of OTHER writes to the volume, and
  # the cost of that is a seed admitted or refused one reading late near the floor. A refused seed
  # keeps the shard at its stored object (the pre-A2 RPO), and an admitted one is still bounded by
  # the next reading.
  #
  # A HUNG `df` IS BOUNDED AND NEVER STACKED (expert review 2026-11-07 #76, the follower half; the
  # poller half is `Measurements.bounded_read/2`). The read used to run in a `Task` that was
  # `Task.shutdown(:brutal_kill)`ed at the timeout and released its claim, so the next reader started
  # a NEW `df` — but killing the Elixir process does not end an OS `df` stuck in an uninterruptible
  # `statfs` (an EBS / NFS brown-out), so every second of the stall added one more D-state process.
  # Now one process OWNS the refresh and holds the claim for as long as the read is alive: the claim
  # row names its owner, a refresh is started only when that owner is dead, so there is no expiry
  # to outwait and no second `df` while the first is stuck. Past `Measurements.disk_read_timeout_ms/0`
  # (2 s) the owner publishes `:error` and goes on waiting; if the `df` ever answers, the late reading
  # replaces the `:error`.
  #
  # `:error` (unknown free space) FAILS OPEN: `headroom?/1` admits and `below_floor?/1` does not
  # refuse, exactly as for a volume that cannot be read at all. Refusing instead was weighed and
  # rejected: a `statfs` that hangs means THIS volume is stalled, where every write the follower makes
  # blocks whether or not the seed was admitted, so refusing protects nothing the stall has not
  # already taken, while it turns an unread gauge into lost redundancy (every shard that needs a
  # seed stays at its stored object, the pre-A2 RPO) for as long as `df` is slow — including a `df`
  # that is slow for a reason other than the volume. The block is logged once a minute, so the
  # fail-open is not silent.
  #
  # Only with NO reading at all (the first begin after boot, before the boot refresh lands) does a
  # reader wait: it starts the refresh if none is running and waits for the answer, at most one
  # read timeout.
  defp disk_free(tab, dir) do
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(tab, {:disk, dir}) do
      [{_, at, result}] ->
        if now - at >= @disk_cache_ms, do: refresh_disk_async(tab, dir)
        result

      [] ->
        first_disk_reading(tab, dir)
    end
  rescue
    ArgumentError -> :error
  end

  @doc """
  Record a reading of `dir`'s free space (the node poller's, `Measurements.disk/0`, or this
  module's own refresh) in the cache row `disk_free/2` reads.
  """
  @spec note_disk_reading(
          :ets.table(),
          String.t(),
          {:ok, non_neg_integer(), non_neg_integer() | nil} | :error
        ) :: :ok
  def note_disk_reading(tab, dir, result) do
    :ets.insert(tab, {{:disk, dir}, System.monotonic_time(:millisecond), result})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # Starts the one background read of `dir`'s volume unless its owner is still alive.
  #
  # The owner is spawned parked and claimed BY PID before it is released, so the claim names the
  # process that holds the read. The lookup first keeps a stuck `df` from costing a spawn per begin.
  def refresh_disk_async(tab, dir) do
    if disk_refresh_running?(tab, dir) do
      :ok
    else
      owner =
        spawn(fn ->
          receive do
            :go -> run_disk_refresh(tab, dir)
          end
        end)

      if claim_disk_refresh(tab, dir, owner),
        do: send(owner, :go),
        else: Process.exit(owner, :kill)

      :ok
    end
  end

  defp disk_refresh_running?(tab, dir) do
    case :ets.lookup(tab, {:disk_refresh, dir}) do
      [{_, owner}] -> Process.alive?(owner)
      [] -> false
    end
  end

  # The claim is the row `{{:disk_refresh, dir}, owner_pid}`. A row whose owner is dead (killed
  # without reaching `release_disk_claim/2`) is taken over; a live owner is never displaced, however
  # long its `df` takes.
  defp claim_disk_refresh(tab, dir, owner) do
    key = {:disk_refresh, dir}

    :ets.insert_new(tab, {key, owner}) or
      case :ets.lookup(tab, key) do
        [{_, holder} = claim] ->
          not Process.alive?(holder) and
            :ets.delete_object(tab, claim) and :ets.insert_new(tab, {key, owner})

        [] ->
          :ets.insert_new(tab, {key, owner})
      end
  end

  defp release_disk_claim(tab, dir) do
    :ets.delete_object(tab, {{:disk_refresh, dir}, self()})
  rescue
    # The follower (and with it the table) stopped mid-read.
    ArgumentError -> true
  end

  # The claim holder. The read itself runs in a SEPARATE process (`reader`): when `df` is stuck in
  # `statfs` that process cannot be interrupted, so the owner, which must publish the `:error` at the
  # timeout, cannot be the one blocked in it. The owner outlives the timeout until the reader ends,
  # which is what keeps the claim held while the OS `df` is.
  defp run_disk_refresh(tab, dir) do
    owner = self()
    ref = make_ref()
    at = System.monotonic_time(:millisecond)
    {reader, monitor} = spawn_monitor(fn -> send(owner, {ref, read_disk_free(dir)}) end)

    receive do
      {^ref, result} ->
        :ets.insert(tab, {{:disk, dir}, at, result})

      # The reader crashed: unknown, like an unreadable volume.
      {:DOWN, ^monitor, :process, ^reader, _reason} ->
        :ets.insert(tab, {{:disk, dir}, at, :error})
    after
      Fathom.Admin.Measurements.disk_read_timeout_ms() ->
        :ets.insert(tab, {{:disk, dir}, System.monotonic_time(:millisecond), :error})
        note_disk_read_blocked(dir)

        receive do
          {^ref, result} -> :ets.insert(tab, {{:disk, dir}, at, result})
          {:DOWN, ^monitor, :process, ^reader, _reason} -> :ok
        end
    end

    :ok
  rescue
    # The follower (and with it the table) stopped mid-read.
    ArgumentError -> :ok
  after
    release_disk_claim(tab, dir)
  end

  defp note_disk_read_blocked(dir) do
    case Fathom.LogLimiter.check(:follower_disk_read_blocked, 1, 60_000) do
      :suppress ->
        :ok

      _log ->
        Logger.warning(
          "replication follower: the free-space read (`df`) of #{dir} did not answer in " <>
            "#{Fathom.Admin.Measurements.disk_read_timeout_ms()} ms — a hung `statfs`, usually a " <>
            "storage stall. Seed admission treats the volume's free space as UNKNOWN and admits " <>
            "(fail open) until the read answers; no second `df` is started meanwhile. " <>
            "(Logged once per minute.)"
        )
    end
  end

  defp first_disk_reading(tab, dir) do
    refresh_disk_async(tab, dir)

    deadline =
      System.monotonic_time(:millisecond) + Fathom.Admin.Measurements.disk_read_timeout_ms() + 100

    await_disk_reading(tab, dir, deadline)
  end

  # Another reader is taking the first reading. Fail OPEN (`:error`) if it does not land in time,
  # as for an unreadable volume.
  defp await_disk_reading(tab, dir, deadline) do
    case :ets.lookup(tab, {:disk, dir}) do
      [{_, _at, result}] ->
        result

      [] ->
        if System.monotonic_time(:millisecond) >= deadline do
          :error
        else
          receive do
          after
            1 -> :ok
          end

          await_disk_reading(tab, dir, deadline)
        end
    end
  end

  # Bytes AND inodes from ONE `df` (`Measurements.volume_readings/1`, expert review 2026-10-22 r8
  # #10). An unreadable inode count is `nil` (the inode floor is then skipped, fail-open like the
  # byte check) and never costs the byte answer. When that combined read cannot answer, the old
  # pair: bytes from `:disksup`, inodes from `df -Pi` (r7 #24). UNBOUNDED by design: it runs in the
  # refresh's reader process, which `run_disk_refresh/2` waits on under the timeout.
  defp read_disk_free(dir) do
    case Fathom.Admin.Measurements.volume_readings([dir]) do
      [{:ok, %{free_bytes: free, free_inodes: free_inodes}}] -> {:ok, free, free_inodes}
      # `df` answered something this parser does not read: try the pair.
      _unreadable -> read_disk_free_pair(dir)
    end
  end

  defp read_disk_free_pair(dir) do
    with {:ok, %{free_bytes: free}} <- Fathom.Admin.Measurements.disk_info(dir) do
      free_inodes =
        case Fathom.Admin.Measurements.inode_info(dir) do
          {:ok, %{free_inodes: n}} -> n
          _ -> nil
        end

      {:ok, free, free_inodes}
    else
      _ -> :error
    end
  end

  # INODE FLOOR (expert review 2026-10-21 r7 #24). A replica is 5 files, three of them sidecars under
  # 64 bytes, so a follower of small tenants runs out of inodes (ext4: one per 16 KiB by default)
  # while the byte floor still reads healthy, and every seed and sidecar write then fails ENOSPC.
  # Refusing a new seed below this many free inodes is the same back-pressure as the byte floor. No
  # per-seed reservation, unlike bytes: a seed creates at most ~5 files, which concurrent seeds cannot
  # make matter against a floor of tens of thousands.
  defp inodes_ok?(nil), do: true

  defp inodes_ok?(free_inodes), do: free_inodes >= disk_free_floor_inodes()

  defp disk_free_floor_inodes,
    do: Application.get_env(:fathom, :replication_disk_free_floor_inodes, 50_000)

  defp disk_free_floor_bytes,
    do: Application.get_env(:fathom, :replication_disk_free_floor_bytes, 1024 * 1024 * 1024)
end
