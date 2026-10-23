defmodule Fathom.Admin.Measurements do
  @moduledoc """
  Periodic gauge measurements for the metrics layer, driven by the `Fathom.Telemetry` poller.
  Each emits a `:telemetry` event the Prometheus reporter turns into a gauge (and the dashboard's
  `Fathom.Admin.MetricsCollector` reads the same values). Cheap, node-local, off the hot path.

  Storage footprint (`Fathom.Shard.Storage.stored_usage/0`) is deliberately **not** here — it can
  be a full S3 LIST, so the collector polls it on a slow, separate cadence and caches it.
  """

  require Logger

  alias Fathom.Admin.FlushWatermark
  alias Fathom.Shard.WriteCounter

  @doc "Live SQLite connections on this node (perf review 2026-10-14 #16)."
  @spec connections() :: :ok
  def connections do
    :telemetry.execute(
      [:fathom, :node, :connections],
      %{live: Fathom.Shard.Connection.live_count()},
      %{}
    )
  end

  @doc """
  Process- and port-table headroom for this node.

  Expert review 2026-07-24 #2: neither limit degrades gracefully. Exhausting `+P` makes `spawn`
  throw `system_limit` (a supervisor start failure — i.e. refused checkouts); exhausting `+Q` makes
  `gen_tcp:accept` return `{:error, :system_limit}` and the listener stops accepting. Both are hard
  availability cliffs, and production materializes 3–4 processes and 1 port per served shard, so at
  the demonstrated densities they are reachable. `rel/vm.args.eex` raises both well clear, and these
  gauges make the approach visible so it can be alerted on rather than discovered as an outage.
  """
  @spec vm_limits() :: :ok
  def vm_limits do
    processes = :erlang.system_info(:process_count)
    process_limit = :erlang.system_info(:process_limit)
    ports = :erlang.system_info(:port_count)
    port_limit = :erlang.system_info(:port_limit)

    :telemetry.execute(
      [:fathom, :node, :vm_limits],
      %{
        processes: processes,
        process_limit: process_limit,
        process_used_ratio: safe_ratio(processes, process_limit),
        ports: ports,
        port_limit: port_limit,
        port_used_ratio: safe_ratio(ports, port_limit)
      },
      %{}
    )
  end

  defp safe_ratio(_used, limit) when not is_integer(limit) or limit <= 0, do: 0.0
  defp safe_ratio(used, limit), do: used / limit

  @doc """
  The node-wide concurrent-flush gate: how many slots are in flight, and the cap.

  Added because the gate had NO observability at all (expert review 2026-08-20 #15). A leaked slot
  — a coordinator killed mid-flush — is permanent without `FlushGate.sweep/0`, and the cap is
  single digits, so a handful of leaks makes `try_acquire/0` answer `:full` forever and every dirty
  shard on the node stops flushing. Nothing else surfaces that: `[:fathom, :shard, :flush, :failed]`
  only fires for a flush that actually RAN, so the node goes quiet rather than loud, and the RPO
  grows unbounded behind the silence.

  `in_flight` at or above `cap` for a sustained period is the alertable condition.
  """
  @spec flush_gate() :: :ok
  def flush_gate do
    case Fathom.Shard.FlushGate.cap() do
      cap when is_integer(cap) ->
        :telemetry.execute(
          [:fathom, :shard, :flush_gate],
          %{
            in_flight: Fathom.Shard.FlushGate.in_flight(),
            cap: cap,
            # Monotonic refusal count (expert review 2026-08-26 #16). `in_flight` at cap says the
            # gate is BUSY; it does not say anyone was turned away, or how often. A rising refusal
            # rate with a flat `in_flight` is the busy-poll condition, and nothing could see it.
            refusals: Fathom.Shard.FlushGate.refusals()
          },
          %{}
        )

      _ ->
        # No cap configured: the gate is off and the counter is never touched.
        :ok
    end

    :ok
  end

  @sched_key {__MODULE__, :scheduler_sample}

  @doc """
  Run-queue lengths and scheduler utilisation (expert review 2026-10-15 #15).

  Every query runs in an exqlite dirty-IO NIF and `FlushGate` sizes against the dirty-IO pool, yet
  nothing showed whether that pool, the dirty-CPU pool or the normal schedulers were saturated.

    * Run queues (`[:fathom, :node, :run_queue]`: `normal`, `dirty_cpu`, `dirty_io`) are
      instantaneous lengths from `:erlang.statistics(:run_queue_lengths_all)` — normal queues
      first, then the dirty-CPU queue, then the dirty-IO queue.
    * Utilisation (`[:fathom, :node, :scheduler_util]`, ratios 0..1 averaged per kind) is
      `:scheduler.utilization/2` between this tick's `:scheduler.sample_all/0` and the previous
      tick's. The previous sample lives in the calling (poller) process's dictionary — no table,
      no extra process. The first call has no baseline and emits queues only.

  `scheduler_wall_time` accounting stays enabled once `sample_all` turns it on; its cost was
  measured before enabling it here (see the commit message for the method and numbers).
  """
  @spec schedulers() :: :ok
  def schedulers do
    {normal, dirty_cpu, dirty_io} = run_queues()

    :telemetry.execute(
      [:fathom, :node, :run_queue],
      %{normal: normal, dirty_cpu: dirty_cpu, dirty_io: dirty_io},
      %{}
    )

    cur = :scheduler.sample_all()

    case Process.put(@sched_key, cur) do
      nil ->
        :ok

      prev ->
        utils = scheduler_utils(:scheduler.utilization(prev, cur))

        if utils != %{}, do: :telemetry.execute([:fathom, :node, :scheduler_util], utils, %{})
    end

    :ok
  end

  defp run_queues do
    lengths = :erlang.statistics(:run_queue_lengths_all)
    n_normal = :erlang.system_info(:schedulers)
    {normal, rest} = Enum.split(lengths, n_normal)

    case rest do
      [cpu, io] -> {Enum.sum(normal), cpu, io}
      [cpu] -> {Enum.sum(normal), cpu, 0}
      _ -> {Enum.sum(normal), 0, 0}
    end
  end

  # `:scheduler.utilization/2` over a `sample_all` pair returns `{:total, u, pct}`,
  # `{:weighted, u, pct}` and `{kind, id, u, pct}` rows with kind :normal | :cpu | :io. Average the
  # per-scheduler rows by kind; the whole-VM rows are not needed.
  @doc false
  @spec scheduler_utils([tuple()]) :: %{optional(atom()) => float()}
  def scheduler_utils(rows) do
    rows
    |> Enum.flat_map(fn
      {kind, _id, u, _pct} when kind in [:normal, :cpu, :io] and is_number(u) -> [{kind, u}]
      _ -> []
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Map.new(fn {kind, us} -> {util_key(kind), Enum.sum(us) / length(us)} end)
  end

  defp util_key(:normal), do: :normal
  defp util_key(:cpu), do: :dirty_cpu
  defp util_key(:io), do: :dirty_io

  @doc """
  The `:erlang.memory/0` breakdown as gauges (processes, binary, ets, code, atom, system), plus the
  node total gauge (`[:fathom, :node, :memory]`) from the same call (expert review 2026-10-17 #33:
  it used to be a second `:erlang.memory` call per poll).
  """
  @spec memory_breakdown() :: :ok
  def memory_breakdown do
    mem = :erlang.memory()
    :telemetry.execute([:fathom, :node, :memory], %{total: Keyword.fetch!(mem, :total)}, %{})

    breakdown =
      Map.new([:processes, :binary, :ets, :code, :atom, :system], &{&1, Keyword.fetch!(mem, &1)})

    :telemetry.execute(
      [:fathom, :node, :memory_breakdown],
      Map.merge(breakdown, carrier_gauges()),
      %{}
    )
  end

  # Carrier-level gauges, so the density numbers cannot be misread (expert review 2026-10-21 r7 #3).
  #
  # Every figure above is `:erlang.memory/0`, which sums allocated BLOCKS. SQLite's page caches
  # live in `driver_alloc` multiblock carriers, interleaved across connections, and an ERTS carrier
  # goes back to the OS only when it is completely empty. After a churn (most connections closed,
  # `PRAGMA shrink_memory` run) the blocks are freed and `:erlang.memory`, `served_kb_per_shard`
  # and `system` all fall while the process RSS stays at the high-water mark. These gauges put the
  # three quantities side by side:
  #
  #   * `allocated_blocks` / `allocated_carriers` - sum over every `alloc_util` allocator
  #   * `driver_blocks` / `driver_carriers` - the `driver_alloc` slice (all SQLite allocations)
  #   * `driver_fragmentation` - driver carriers / driver blocks (1.0 = no reusable-but-held slack)
  #   * `rss` - the OS resident set from `/proc/self/status` (Linux only; absent elsewhere)
  #
  # Cost: ~1 ms per poll on a 12-scheduler VM (`allocator_sizes` for the 10 allocators).
  @spec carrier_gauges() :: %{optional(atom()) => number()}
  defp carrier_gauges do
    # ONE walk (expert review 2026-10-22 r8 #24b): `driver_alloc` is one of the
    # `alloc_util_allocators`, and querying it a second time cost another cross-scheduler
    # `allocator_sizes` collection per poll for numbers the first walk already had.
    {{carriers, blocks}, {driver_carriers, driver_blocks}} =
      alloc_sizes(:erlang.system_info(:alloc_util_allocators))

    gauges = %{
      allocated_carriers: carriers,
      allocated_blocks: blocks,
      driver_carriers: driver_carriers,
      driver_blocks: driver_blocks
    }

    gauges =
      if driver_blocks > 0,
        do: Map.put(gauges, :driver_fragmentation, driver_carriers / driver_blocks),
        else: gauges

    case Fathom.FS.read("/proc/self/status") do
      {:ok, status} ->
        case rss_bytes(status) do
          nil -> gauges
          rss -> Map.put(gauges, :rss, rss)
        end

      {:error, _} ->
        gauges
    end
  end

  # `{{carriers, blocks} over every allocator, {carriers, blocks} of driver_alloc alone}`.
  defp alloc_sizes(allocators) do
    Enum.reduce(allocators, {{0, 0}, {0, 0}}, fn alloc, {{c, b}, driver} ->
      case :erlang.system_info({:allocator_sizes, alloc}) do
        instances when is_list(instances) ->
          {ic, ib} = sum_allocator_sizes(instances)
          {{c + ic, b + ib}, if(alloc == :driver_alloc, do: {ic, ib}, else: driver)}

        _disabled ->
          {{c, b}, driver}
      end
    end)
  end

  @doc """
  Sums `{carriers_size, blocks_size}` over the instances of one `:erlang.system_info({:allocator_sizes, a})`
  result. Carriers are the memory ERTS holds from the OS (multiblock, single-block and pooled
  carriers); blocks are the live allocations inside them. Public only so the parser is testable
  against a fixed shape.
  """
  @spec sum_allocator_sizes([term()]) :: {non_neg_integer(), non_neg_integer()}
  def sum_allocator_sizes(instances) do
    Enum.reduce(instances, {0, 0}, fn
      {:instance, _n, areas}, acc ->
        Enum.reduce(areas, acc, fn
          {area, props}, {c, b} when area in [:mbcs, :sbcs, :mbcs_pool, :sbcs_pool] ->
            {c + carriers_of(props), b + blocks_of(props)}

          _other, acc ->
            acc
        end)

      _other, acc ->
        acc
    end)
  end

  defp carriers_of(props) do
    case List.keyfind(props, :carriers_size, 0) do
      {:carriers_size, cur, _, _} -> cur
      {:carriers_size, cur} -> cur
      _ -> 0
    end
  end

  defp blocks_of(props) do
    case List.keyfind(props, :blocks, 0) do
      {:blocks, types} ->
        Enum.reduce(types, 0, fn
          {_type, sizes}, acc when is_list(sizes) ->
            case List.keyfind(sizes, :size, 0) do
              {:size, cur, _, _} -> acc + cur
              {:size, cur} -> acc + cur
              _ -> acc
            end

          _other, acc ->
            acc
        end)

      _ ->
        0
    end
  end

  @doc """
  The `VmRSS` line of a `/proc/<pid>/status` body, in bytes; `nil` when absent (non-Linux, or a
  truncated read). Public only so the parser is testable.
  """
  @spec rss_bytes(binary()) :: non_neg_integer() | nil
  def rss_bytes(status) when is_binary(status) do
    case Regex.run(~r/^VmRSS:\s+(\d+)\s+kB$/m, status) do
      [_, kb] -> String.to_integer(kb) * 1024
      _ -> nil
    end
  end

  @doc "Directory.Recorder buffer depth and flush freshness (expert review 2026-10-15 #17)."
  @spec recorder() :: :ok
  def recorder do
    s = Fathom.Directory.Recorder.stats()

    measurements = %{buffered: s.buffered, flush_buffered: s.flush_buffered}

    measurements =
      if s.last_flush_age_ms,
        do: Map.put(measurements, :last_flush_age_ms, s.last_flush_age_ms),
        else: measurements

    :telemetry.execute([:fathom, :directory, :recorder], measurements, %{})
  end

  @doc """
  Local-disk headroom for the directories fathom writes to (expert review 2026-08-01 #36).

  Nothing in the metrics layer read the filesystem before this: `fathom.storage.bytes` is *S3*
  usage, and the then warm-follower cache (removed 2026-09-14) — the one component deliberately
  sized to fill disk — was budgeted in shard **count**, which is 8 MB or 2 TB depending on tenant
  size. The A2 replica store (`replica` below) is the disk consumer that grows like that now. `docs/runbooks/operations.md` ranks disk-full a top-four incident and its prevention
  step is "alert on disk %", a signal fathom did not emit.

  **Why it matters more than an ordinary capacity gauge:** when the volume fills, every cold-open
  `pull` fails AND every dirty shard's `VACUUM INTO` fails, so writes keep being **acked** and can
  never be made durable — the RPO contract goes unbounded. The symptoms that surface
  (`fathom.shard.flush.failed` climbing, `fathom.durability.oldest_age_ms` growing) are the same
  ones an S3 credential or reachability problem produces, so without this gauge the diagnostic path
  points away from the actual cause.

  Emits per directory (`data` = `SHARD_DATA_DIR`, `replica` = `REPLICATION_DIR`), tagged so a fleet
  dashboard can break them out: `free_bytes`, `total_bytes`, `used_ratio` — the last matching the
  `process_used_ratio` / `port_used_ratio` convention above. (The `warm` dir gauge was removed
  2026-09-14 with the WarmFollower retirement.)

  `replica` joined 2026-08-10 with the follower listener. A node acting as somebody's follower
  stores a **full copy of every shard it follows**, so it is a third disk consumer of exactly the
  kind this gauge exists for — and the one with the least warning, because it grows from other
  nodes' write traffic rather than from anything happening locally. It is only non-nil on a node
  that actually listens; a pure primary reports just `data`.

  Best-effort: a path that does not exist yet (first boot before the data dir is created) is skipped
  rather than reported as 0 free, which would read as a full disk.
  """
  @spec disk() :: :ok
  def disk do
    dirs =
      for {_label, path} = d <- [data_dir_entry(), replica_dir_entry()], is_binary(path), do: d

    # ONE `df` for every directory, bytes and inodes together (expert review 2026-10-22 r8 #10).
    # This used to be `disk_info/1` (`disksup`, which runs `df` through its port) plus
    # `inode_measurements/1` (`df -Pi`) per directory: four subprocesses per poll on a follower.
    # A directory the combined read cannot answer falls back to the old per-directory pair, so a
    # `df` that prints something unexpected costs the saving, never the gauge.
    readings = volume_readings(Enum.map(dirs, &elem(&1, 1)))

    for {{label, path}, reading} <- Enum.zip(dirs, readings),
        {:ok, %{total_bytes: total, free_bytes: free} = r} <- [reading || per_dir_reading(path)] do
      if label == "data", do: put_data_free_bytes(free)

      measurements =
        %{
          free_bytes: free,
          total_bytes: total,
          used_ratio: safe_ratio(total - free, total)
        }
        |> Map.merge(inode_gauges(r))

      :telemetry.execute([:fathom, :node, :disk], measurements, %{dir: label})
    end

    :ok
  end

  defp data_dir_entry, do: {"data", Fathom.Shard.data_dir()}
  defp replica_dir_entry, do: {"replica", replica_dir()}

  # The old per-directory path (`disksup` bytes + `df -Pi` inodes), in the combined shape.
  defp per_dir_reading(path) do
    with {:ok, bytes} <- disk_info(path) do
      inodes =
        case inode_info(path) do
          {:ok, i} -> i
          :error -> %{total_inodes: nil, free_inodes: nil}
        end

      {:ok, Map.merge(bytes, inodes)}
    end
  end

  # INODES, NOT ONLY BYTES (expert review 2026-10-21 r7 #24). An A2 replica is 5 files (`.db`,
  # `-wal` and three sidecars of under 64 bytes), so a follower of small tenants exhausts ext4's
  # inodes (16 KiB per inode by default) before its bytes: a replica averaging under ~80 KiB does.
  # The volume then refuses every new file with ENOSPC while `used_ratio` reads healthy, the same
  # misdirection the byte gauge exists to remove. Absent (not 0) when the inode count cannot be read
  # or the filesystem has no fixed inode table (btrfs, ZFS report 0 total), so no alert fires on a
  # number that means nothing.
  defp inode_gauges(%{total_inodes: total, free_inodes: free})
       when is_integer(total) and total > 0 and is_integer(free) do
    %{
      free_inodes: free,
      total_inodes: total,
      inodes_used_ratio: safe_ratio(total - free, total)
    }
  end

  defp inode_gauges(_reading), do: %{}

  @typedoc "One volume's free space: bytes always, inodes `nil` when the filesystem has none."
  @type volume_reading :: %{
          mount: String.t(),
          total_bytes: non_neg_integer(),
          free_bytes: non_neg_integer(),
          total_inodes: non_neg_integer() | nil,
          free_inodes: non_neg_integer() | nil
        }

  @doc """
  Bytes AND inodes for the volume holding each of `paths`, from ONE `df` subprocess (expert review
  2026-10-22 r8 #10). Returns a list aligned with `paths`: `{:ok, reading}`, or `nil` for a path
  this read could not answer (the caller falls back to `disk_info/1` + `inode_info/1`).

  Linux (GNU coreutils, the release image's Debian): `df -B1 --output=size,avail,itotal,iavail,target`.
  Elsewhere (BSD/macOS): `df -Pki`, which prints the byte AND inode columns in one row. Columns
  are found by header name; a row whose numbers do not parse (an unexpected `df`, a filesystem
  name with spaces) is `nil` rather than a guess, for the same reason as `disk_info/1`. Free bytes
  are the `avail` column, as `disksup` reports them. Spawns one OS process: poller use only.
  """
  @spec volume_readings([String.t()]) :: [{:ok, volume_reading()} | nil]
  def volume_readings([]), do: []

  def volume_readings(paths) do
    args = Enum.map(paths, &existing_ancestor/1)
    flavor = df_flavor()

    {cmd_args, parse} =
      case flavor do
        :gnu -> {["-B1", "--output=size,avail,itotal,iavail,target", "--" | args], :gnu}
        :bsd -> {["-Pki" | args], :bsd}
      end

    case System.cmd("df", cmd_args, stderr_to_stdout: true) do
      {out, 0} -> align(parse_df_volumes(out, parse), length(paths))
      _ -> List.duplicate(nil, length(paths))
    end
  rescue
    _ -> List.duplicate(nil, length(paths))
  end

  defp df_flavor do
    case :os.type() do
      {:unix, :linux} -> :gnu
      _ -> :bsd
    end
  end

  # `df` prints one row per argument, in argument order. Any other count means the output is not
  # what this parser understands, so nothing is trusted.
  defp align(rows, n) when length(rows) == n, do: rows
  defp align(_rows, n), do: List.duplicate(nil, n)

  @doc false
  # Rows of `df -B1 --output=size,avail,itotal,iavail,target` (`:gnu`) or `df -Pki` (`:bsd`), one
  # entry per data row: `{:ok, reading}` or `nil`. Public for the test.
  @spec parse_df_volumes(String.t(), :gnu | :bsd) :: [{:ok, volume_reading()} | nil]
  def parse_df_volumes(out, flavor) do
    case String.split(out, "\n", trim: true) do
      [header | rows] ->
        cols = header |> String.downcase() |> String.split()
        Enum.map(rows, &parse_df_row(String.split(&1), cols, flavor))

      [] ->
        []
    end
  end

  # GNU `--output` order is fixed by the argument: size avail itotal iavail target (no filesystem
  # column), so the first four fields are the numbers and the rest is the mount point.
  defp parse_df_row([size, avail, itotal, iavail | mount], _cols, :gnu) when mount != [] do
    with {total, ""} when total > 0 <- Integer.parse(size),
         {free, ""} when free >= 0 <- Integer.parse(avail) do
      {:ok, reading(Enum.join(mount, " "), total, free, int_or_nil(itotal), int_or_nil(iavail))}
    else
      _ -> nil
    end
  end

  defp parse_df_row(fields, cols, :bsd) do
    with total_at when is_integer(total_at) <- Enum.find_index(cols, &(&1 == "1024-blocks")),
         avail_at when is_integer(avail_at) <- Enum.find_index(cols, &(&1 == "available")),
         used_at when is_integer(used_at) <- Enum.find_index(cols, &(&1 == "iused")),
         ifree_at when is_integer(ifree_at) <- Enum.find_index(cols, &(&1 == "ifree")),
         mount_at when is_integer(mount_at) <- Enum.find_index(cols, &(&1 == "mounted")),
         # "Mounted on" is two header words and the last column, so a row has exactly one field
         # fewer than the header has words. Any other count is a space in a filesystem name or a
         # mount point, which would shift every index: unreadable, not guessed.
         true <- length(fields) == length(cols) - 1,
         {total_kb, ""} when total_kb > 0 <- Integer.parse(Enum.at(fields, total_at)),
         {avail_kb, ""} when avail_kb >= 0 <- Integer.parse(Enum.at(fields, avail_at)) do
      iused = int_or_nil(Enum.at(fields, used_at))
      ifree = int_or_nil(Enum.at(fields, ifree_at))
      itotal = if iused && ifree, do: iused + ifree

      mount = Enum.at(fields, mount_at)
      {:ok, reading(mount, total_kb * 1024, avail_kb * 1024, itotal, ifree)}
    else
      _ -> nil
    end
  end

  defp parse_df_row(_fields, _cols, _flavor), do: nil

  defp reading(mount, total, free, itotal, ifree) do
    # No fixed inode table (btrfs, ZFS: 0 total) reads as "unknown", never as "0 free".
    {itotal, ifree} =
      if is_integer(itotal) and itotal > 0 and is_integer(ifree),
        do: {itotal, ifree},
        else: {nil, nil}

    %{
      mount: mount,
      total_bytes: total,
      free_bytes: free,
      total_inodes: itotal,
      free_inodes: ifree
    }
  end

  defp int_or_nil(nil), do: nil

  defp int_or_nil(s) do
    case Integer.parse(s) do
      {n, ""} when n >= 0 -> n
      _ -> nil
    end
  end

  @doc """
  Free/total inodes for the filesystem holding `path` (expert review 2026-10-21 r7 #24), or
  `:error` when they cannot be read or the filesystem has no fixed inode count.

  `:disksup` reports bytes only and the BEAM has no `statvfs`, so this runs `df -Pi` (POSIX output,
  inode columns) and reads the `IUsed`/`IFree` columns by their header names, which GNU (Linux:
  `Inodes IUsed IFree`) and BSD (macOS: `iused ifree`) both print. Spawns one OS process: call it
  from a poller or behind a cache, never per request. `:error` rather than a guess, for the same
  reason as `disk_info/1`.
  """
  @spec inode_info(String.t()) ::
          {:ok, %{total_inodes: non_neg_integer(), free_inodes: non_neg_integer()}} | :error
  def inode_info(path) do
    case System.cmd("df", ["-Pi", existing_ancestor(path)], stderr_to_stdout: true) do
      {out, 0} -> parse_df_inodes(out)
      _ -> :error
    end
  rescue
    # No `df` on PATH (`System.cmd` raises `:enoent`), or anything else unreadable.
    _ -> :error
  end

  @doc false
  # The header-indexed `IUsed`/`IFree` columns of `df -Pi` output. Public for the test.
  @spec parse_df_inodes(String.t()) ::
          {:ok, %{total_inodes: non_neg_integer(), free_inodes: non_neg_integer()}} | :error
  def parse_df_inodes(out) do
    with [header, row | _] <- String.split(out, "\n", trim: true),
         cols = header |> String.downcase() |> String.split(),
         used_at when is_integer(used_at) <- Enum.find_index(cols, &(&1 == "iused")),
         free_at when is_integer(free_at) <- Enum.find_index(cols, &(&1 == "ifree")),
         fields = String.split(row),
         {used, ""} when used >= 0 <- Integer.parse(Enum.at(fields, used_at, "")),
         {free, ""} when free >= 0 <- Integer.parse(Enum.at(fields, free_at, "")),
         total when total > 0 <- used + free do
      {:ok, %{total_inodes: total, free_inodes: free}}
    else
      _ -> :error
    end
  end

  # The data volume's free bytes as the poller last read them, for the flush preflight (expert review
  # 2026-10-12 #26). `:disksup.get_disk_info/1` runs `df` through a port on every call, so a flush
  # must never call it; it reads this cache instead. Three `:atomics` cells (free bytes, monotonic ms
  # of the sample, a valid flag; monotonic time can be negative, so it cannot double as the flag) behind one `:persistent_term` key that is written ONCE (an atomics ref never
  # changes, so refreshing a reading is a lock-free `put`, not a persistent_term update, which
  # would trigger a global GC scan of every process).
  @free_key {__MODULE__, :data_free}
  # A reading older than this is treated as unknown: a stalled or stopped poller must not freeze an
  # old "disk is nearly full" (or "plenty free") answer into every flush.
  @free_max_age_ms 120_000

  @doc """
  The data volume's free bytes from the poller's last reading, or `:unknown` when the poller has not
  run, or its reading is older than #{div(@free_max_age_ms, 1000)} s. Cheap (two atomics reads):
  safe on the flush path.
  """
  @spec data_free_bytes() :: {:ok, non_neg_integer()} | :unknown
  def data_free_bytes do
    with ref when ref != nil <- :persistent_term.get(@free_key, nil),
         1 <- :atomics.get(ref, 3),
         true <- System.monotonic_time(:millisecond) - :atomics.get(ref, 2) <= @free_max_age_ms do
      {:ok, :atomics.get(ref, 1)}
    else
      _ -> :unknown
    end
  end

  @doc """
  Records the data volume's free bytes. Called by `disk/0` on every poll; public so a test can stand
  in for the poller.
  """
  @spec put_data_free_bytes(non_neg_integer()) :: :ok
  def put_data_free_bytes(free) when is_integer(free) and free >= 0 do
    ref =
      case :persistent_term.get(@free_key, nil) do
        nil ->
          ref = :atomics.new(3, signed: true)
          :persistent_term.put(@free_key, ref)
          ref

        ref ->
          ref
      end

    :atomics.put(ref, 1, free)
    :atomics.put(ref, 2, System.monotonic_time(:millisecond))
    :atomics.put(ref, 3, 1)
    :ok
  end

  @doc "Forgets the cached free-space reading (a test's cleanup)."
  @spec clear_data_free_bytes() :: :ok
  def clear_data_free_bytes do
    case :persistent_term.get(@free_key, nil) do
      nil -> :ok
      ref -> :atomics.put(ref, 3, 0)
    end
  end

  @doc """
  Free/total bytes for the filesystem holding `path`, or `:error` if it cannot be read.

  `:disksup.get_disk_info/1` reports the mount containing the path as
  `[{mount, total_kb, available_kb, capacity_percent}]`. KB there is 1024-byte blocks. Returns
  `:error` rather than raising or guessing: this feeds a gauge and a back-pressure decision, and a
  fabricated number in either is worse than an absent one — a wrong "plenty free" disables the
  brake, a wrong "full" makes a healthy follower refuse seeds (`Follower.headroom?/2`).
  """
  @spec disk_info(String.t()) ::
          {:ok, %{mount: String.t(), total_bytes: integer(), free_bytes: integer()}} | :error
  def disk_info(path) do
    # Resolve to the nearest EXISTING ancestor first. `SHARD_DATA_DIR` and `REPLICATION_DIR` are
    # created lazily — the data dir does not exist until the first shard opens — and `disksup`
    # returns nothing for a path that is not there. Without this the gauge is blind on exactly the
    # node state where disk headroom is most worth knowing: a freshly booted node about to pull its
    # working set. The ancestor sits on the same filesystem, which is the quantity being measured.
    case :disksup.get_disk_info(to_charlist(existing_ancestor(path))) do
      [{mount, total_kb, avail_kb, _pct} | _] when is_integer(total_kb) and total_kb > 0 ->
        # `mount` identifies the FILESYSTEM, not the path — two directories reporting the same
        # mount share a volume, and therefore share a free-space budget. That is the question
        # `Application.check_replication_disk!/0` asks, and comparing sizes cannot answer it: two
        # separate volumes of the same size look identical.
        {:ok,
         %{
           mount: List.to_string(mount),
           total_bytes: total_kb * 1024,
           free_bytes: avail_kb * 1024
         }}

      _ ->
        :error
    end
  rescue
    # os_mon not started (a release that trimmed it, or a test env), or the path is unreadable.
    _ -> :error
  catch
    :exit, _ -> :error
  end

  # Gated on `listening?/0` (the `warm` dir gauge was removed 2026-09-14 with the WarmFollower
  # retirement): a node
  # that is not somebody's follower writes nothing here, and reporting the default tmp path would
  # publish a `dir=replica` series for a directory that will never grow — which is worse than
  # absent, because an alert on it would be measuring an unrelated filesystem.
  defp replica_dir do
    if Fathom.Shard.Replication.Fleet.listening?(),
      do: Fathom.Shard.Replication.Follower.default_dir()
  end

  # Walk up until something exists. Bounded by construction: each step drops one path component and
  # `Path.dirname/1` is a fixed point at the root, so the recursion terminates there even for a
  # path that shares no ancestor with anything real.
  #
  # Raw (`Fathom.FS.exists?/1`, expert review 2026-10-22 r8 #10): `File.exists?/1` is a
  # `file_server_2` round trip, and this runs on the seed-admission path.
  defp existing_ancestor(path) do
    cond do
      Fathom.FS.exists?(path) -> path
      Path.dirname(path) == path -> path
      true -> existing_ancestor(Path.dirname(path))
    end
  end

  @doc """
  Durability / RPO gauge: how many open shards hold un-flushed writes, and the oldest such
  shard's RPO age (ms). Derives dirtiness from the published flush watermark exactly as
  `Fathom.Shard.unflushed?/1` does — a `WriteCounter` generation mismatch or a write count past
  the flushed watermark — so it never disagrees with the coordinator's own dirty decision.
  """
  @spec durability() :: :ok
  def durability do
    # O(open shards) walk (a FlushWatermark snapshot copy + one counter read per shard):
    # skip it entirely when the observability layer is off — nothing consumes the event,
    # and the walk scales linearly with density (review 2026-07-23 #30). memory_breakdown/0
    # (node memory) stays ungated (a single :erlang.memory call, effectively free).
    if Fathom.Admin.enabled?(), do: do_durability()
    :ok
  end

  defp do_durability do
    # The poller is the authority: it always walks, and publishes the result for the collector to
    # reuse (expert review 2026-10-17 #16: both used to walk the table independently every 10 s).
    {dirty, oldest} = rpo_walk()
    FlushWatermark.cache_rpo({dirty, oldest})

    # `watermark_rows` vs `open_shards` is the honesty check (expert review 2026-08-26 #17).
    #
    # This function REDUCES over the watermark table, so a missing row contributes nothing and the
    # result is indistinguishable from "that shard is flushed". If the table is ever emptied — a
    # FlushWatermark owner restart is the known way — this emitted a confident `dirty_shards: 0`
    # for a fleet it could not see. #17 makes coordinators re-publish on that restart, which closes
    # the window in milliseconds; these two extra gauges make the window OBSERVABLE rather than
    # trusting that fix to be perfect. `watermark_rows < open_shards` means the answer above is
    # under-reporting, and an alert can say so instead of going quiet.
    rows = FlushWatermark.size()
    open_shards = open_shard_count()

    :telemetry.execute(
      [:fathom, :durability, :rpo],
      %{
        dirty_shards: dirty,
        oldest_age_ms: oldest,
        watermark_rows: rows,
        open_shards: open_shards
      },
      %{}
    )
  end

  @rpo_ttl_ms 9_000

  @doc """
  `{dirty_shards, oldest_age_ms}` from the published flush watermarks, behind a ~9 s cache that
  `durability/0` refreshes every 10 s (expert review 2026-10-17 #16). `MetricsCollector` reads this
  instead of walking the table itself, so the O(open shards) walk runs once per 10 s, not twice.
  """
  @spec rpo_snapshot() :: {non_neg_integer(), integer()}
  def rpo_snapshot do
    case FlushWatermark.cached_rpo(@rpo_ttl_ms) do
      nil ->
        fresh = rpo_walk()
        FlushWatermark.cache_rpo(fresh)
        fresh

      cached ->
        cached
    end
  end

  # Dirtiness derived exactly as `Fathom.Shard.unflushed?/1` does.
  defp rpo_walk do
    now = System.monotonic_time(:millisecond)
    gen = WriteCounter.generation()

    FlushWatermark.fold({0, 0}, fn
      {id, flushed_through, counter_gen, flushed_at}, {dirty, oldest} ->
        if counter_gen != gen or WriteCounter.count(id) > flushed_through do
          {dirty + 1, max(oldest, now - flushed_at)}
        else
          {dirty, oldest}
        end
    end)
  end

  # The poller can run before the shard tree is up (or after it is gone): count zero then.
  defp open_shard_count do
    Fathom.Shards.open_count()
  catch
    :exit, _ -> 0
  end

  @doc """
  Control-plane liveness gauges (expert review #18): the exception counter catches jobs that
  *fail*, but not jobs that *don't run* — a backlogged/paused queue, or a wedged fleet-singleton
  cron (reconcile, rebalance) that silently stops with zero exceptions. Emits, per queue, the
  `available` + `retryable` depth and the oldest runnable job's age; and, per singleton cron, the
  seconds since it last inserted a job (its freshness). A Postgres blip never crashes the poller —
  the gauge just goes stale, itself a signal. Gated onto its own slow poller by `Fathom.Admin`
  (never runs in test / when metrics are off), so this is the one measurement that touches Postgres.
  """
  @spec oban_health() :: :ok
  def oban_health do
    # The computation is shared with the Migrations page (`Fathom.Admin.ObanHealth`, expert review
    # 2026-10-15 #12); this function only turns it into gauges.
    %{queues: queues, crons: crons} = Fathom.Admin.ObanHealth.snapshot()

    for q <- queues do
      :telemetry.execute(
        [:fathom, :oban, :queue],
        %{available: q.available, retryable: q.retryable, oldest_age_ms: q.oldest_age_ms},
        %{queue: q.queue}
      )
    end

    for c <- crons do
      :telemetry.execute([:fathom, :oban, :cron], %{age_ms: c.age_ms}, %{worker: c.worker})
    end

    :ok
  rescue
    e ->
      Logger.warning("oban_health measurement skipped (Postgres unreachable?): #{inspect(e)}")
      :ok
  end

  @doc """
  Quorum-replication follower state (Phase 2 A2).

  Emits `[:fathom, :replication, :followers]` with `configured`, `connected`, `quorum` and
  `slack` (`connected - quorum`).

  **`slack` is the number this exists for.** A commit needs `quorum` acks, so a fleet with exactly
  `quorum` followers connected still succeeds on every write — and is one loss away from every
  write failing. That state is invisible from the outside: latency is normal, no error rate moves,
  and the shard reports healthy right up until it does not. The pre-A2 gauges cannot show it
  either, because they measure S3 and disk.

  A separate `[:fathom, :replication, :degraded]` event was written first and removed:
  `TelemetryCoverageTest` caught that nothing exported it, and it was redundant anyway — `slack`
  is a number, and `== 0` / `< 0` are the two alert conditions
  (`deploy/observability/alert-rules.yml`). A second event carrying strictly less information is
  not a signal, it is a thing to keep in sync. Do not re-add it.

  A no-op when replication is off, so a node that never enabled A2 emits nothing rather than a
  permanently-degraded zero.

  Uses `Fleet.connection_status/0`, **not** `Fleet.health/0`: this rides the 10 s poller, which
  this module's own contract keeps Postgres-free (only the 30 s Oban poller may query the DB). The
  roster's liveness view is the dashboard's, not this gauge's.
  """
  @spec replication() :: :ok
  def replication do
    if Fathom.Shard.Replication.Session.enabled?() do
      status = Fathom.Shard.Replication.Fleet.connection_status()
      quorum = Application.get_env(:fathom, :replication_quorum, 2)
      connected = Enum.count(status, fn {_key, up?} -> up? end)
      slack = connected - quorum

      :telemetry.execute(
        [:fathom, :replication, :followers],
        %{configured: length(status), connected: connected, quorum: quorum, slack: slack},
        %{}
      )

      replication_budget()
    end

    :ok
  end

  # THE LEADING SATURATION SIGNAL, and the reason it exists rather than only a reject counter.
  #
  # Replication does not degrade gracefully as tenant count rises — measured on the rig, 512 -> 1024
  # -> 2048 tenants went 3,340 -> 2,776 -> 258 txn/s, i.e. a 10.8x collapse for the last doubling.
  # An operator cannot see that coming from throughput, because throughput looks fine right up to
  # the cliff (`docs/reviews/a2-flush-interval-2026-08-18.md`).
  #
  # `:overloaded` rejects DO track it cleanly — 0 at 512, ~9k at 1024, ~17k at 2048 — but a reject
  # is a LAGGING signal: by the time it fires the node is already refusing tenant writes. This gauge
  # is the same quantity one step earlier: how full the per-node byte budget is RIGHT NOW, which
  # climbs before anything is refused.
  #
  # Node-level, never per shard. A per-shard tag at fathom's stated scale is cardinality death — the
  # same reasoning that keeps `Fathom.ShardLoad` a read API rather than a metric.
  defp replication_budget do
    alias Fathom.Shard.Replication.Budget

    max = Budget.max_bytes()
    used = Budget.queued()

    # `max` of 0 means the bound is disabled, and a ratio against it is meaningless rather than
    # infinite — report the bytes and leave the ratio at 0 so a dashboard shows "no bound" instead
    # of a division error or a fake 100%.
    ratio = if max > 0, do: used / max, else: 0.0

    :telemetry.execute(
      [:fathom, :replication, :budget],
      %{used_bytes: used, max_bytes: max, used_ratio: ratio},
      %{}
    )
  end
end
