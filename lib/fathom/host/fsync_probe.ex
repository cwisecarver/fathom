defmodule Fathom.Host.FsyncProbe do
  @moduledoc """
  A 1 Hz 4 KiB write + `fdatasync` to a probe file in the shard data dir, recorded as a histogram
  (expert review 2026-10-29 #17c). **Opt-in, off by default (`FSYNC_PROBE=true`).**

  Every primary commit is a `synchronous=FULL` fsync, and the measured tail came from those fsyncs
  waiting behind kernel writeback bursts. This is that wait measured directly, without a tenant in
  the loop: `[:fathom, :node, :fsync_probe]` `duration` (microseconds, write + datasync) feeds
  `fathom.node.fsync_probe.duration`, and a failed round counts in
  `fathom.node.fsync_probe_error.count`. A probe that shows the 30 s / 60 s bursts with default
  `vm.dirty_*` and not with `dirty_background_bytes` set is attributing correctly; one that never
  moves should be dropped (the finding's own falsification test).

  ## Why it is off, and what it costs

  The finding says to falsify the probe on the rig before shipping it on. It adds a small constant
  load (one 4 KiB overwrite and a device cache flush per second, on the data volume) and it blocks
  one dirty-IO thread for as long as that fsync takes. It measures write + datasync of one
  overwritten block, so it sees data-flush queueing behind writeback but NOT the extra
  journal commit a file-extending append pays. Its duration includes any wait for a free dirty-IO
  thread, which is the same wait a tenant statement sees.

  ## Why the probe file cannot be mistaken for a shard

  The file is `<shard_data_dir>/.fsync_probe`. A shard id matches `[a-zA-Z0-9_-]` (`Fathom.ShardId`),
  so no id yields a dotfile, and every enumerator of the data dir skips it by name:
  `Storage.temp_orphan?/1` and `Shard.quarantine_files_in/2` reject a leading `.`, the quarantine /
  tombstone / tenant-erase sweeps all key on a `<id>.db` prefix or `.db.<kind>.` infix, and
  `Retained.reconcile/2` needs the `.db.retained` suffix. `test/fathom/host/fsync_probe_test.exs`
  runs the temp reaper's name predicates over it.
  """
  use GenServer

  require Logger

  @interval_ms 1_000
  @block_bytes 4096
  @file_name ".fsync_probe"

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @doc "The probe file inside `dir` (the shard data dir)."
  @spec probe_path(Path.t()) :: Path.t()
  def probe_path(dir), do: Path.join(dir, @file_name)

  @doc """
  One probe round against `path`: write a 4 KiB block at offset 0, `datasync`, and return the elapsed
  microseconds of both. The file is opened without truncation and closed again each round, so a
  deleted or remounted data dir is picked up on the next one.
  """
  @spec probe(Path.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def probe(path) do
    block = :binary.copy(<<System.unique_integer([:positive])::32>>, div(@block_bytes, 4))
    started = System.monotonic_time(:microsecond)

    with {:ok, fd} <- :file.open(path, [:raw, :read, :write, :binary]) do
      result =
        with :ok <- :file.pwrite(fd, 0, block), do: :file.datasync(fd)

      _ = :file.close(fd)

      case result do
        :ok -> {:ok, System.monotonic_time(:microsecond) - started}
        {:error, _} = err -> err
      end
    end
  end

  @impl true
  def init(opts) do
    state = %{
      dir: Keyword.get_lazy(opts, :dir, &Fathom.Shard.data_dir/0),
      interval_ms: Keyword.get(opts, :interval_ms, @interval_ms),
      failing?: false
    }

    {:ok, state, {:continue, :arm}}
  end

  @impl true
  def handle_continue(:arm, state) do
    _ = File.mkdir_p(state.dir)
    arm(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:probe, state) do
    state = run(state)
    arm(state)
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # Re-armed only after the round returns, so a stalled fsync can never queue a backlog of probes.
  defp arm(%{interval_ms: ms}), do: Process.send_after(self(), :probe, ms)

  # A failing disk would otherwise log once a second; the counter carries the per-round signal and
  # the log carries the transitions.
  defp run(%{dir: dir} = state) do
    path = probe_path(dir)

    case probe(path) do
      {:ok, us} ->
        :telemetry.execute([:fathom, :node, :fsync_probe], %{duration: us}, %{})
        if state.failing?, do: Logger.info("fsync probe: write+datasync to #{path} recovered")
        %{state | failing?: false}

      {:error, reason} ->
        :telemetry.execute([:fathom, :node, :fsync_probe_error], %{count: 1}, %{})

        unless state.failing? do
          Logger.warning("fsync probe: write+datasync to #{path} failed (#{inspect(reason)})")
        end

        %{state | failing?: true}
    end
  end
end
