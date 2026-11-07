defmodule Fathom.Test.CrashFS do
  @moduledoc """
  A crash-consistency double for `Fathom.FS` (expert review 2026-11-07 #85, #27, #83).

  `mix test` cannot cut power, so a test of "this write survives a power cut" used to be a trace of
  "the caller reached `atomic_write`" — replacing the `fsync` with `:ok` kept the whole suite green.
  This module is the model that makes the omission observable: it records every operation
  `Fathom.FS` reports to the `:fs_hook` seam under one directory, tracks what a real filesystem
  would have to persist, and `crash!/1` rewinds the directory to what survives a power loss.

  ## The model (deliberately the conservative reading of POSIX)

    * **File data is durable only after `{:sync_file, path}`.** A path written (`{:wrote, path}`,
      emitted by `Fathom.FS.write/2` and `Fathom.FS.note_written/1`) and not fsynced since is
      ZERO-FILLED by `crash!/1`: the name survives, the bytes do not (the XFS / ext4-delalloc
      outcome review #17 is about). The dirty state follows a file through a rename or link.
    * **A rename or link is durable only after `{:sync_dir, dir}` on its directory.** `crash!/1`
      undoes every rename the directory was never synced for (moving the file back and restoring
      the previous content of the destination, snapshotted when the rename was requested) and
      removes every such link.
    * Not modelled: unlinks, creates of un-renamed files, reordering of data writes between two
      fsyncs. A test that needs them is asking for a different tool.

  Two crash flavours, because a real filesystem may do either: `crash!(pid)` (default,
  `renames: :revert`) reverts the undurable directory entries; `crash!(pid, renames: :keep)` keeps
  every rename and link but still zero-fills unsynced data (the XFS outcome: the name survived, the
  bytes did not). A write that is durable must hold under BOTH; each fsync is only proven necessary
  by the flavour that loses data without it.

  A file nobody wrote through the seam is treated as durable, so fixtures created with `File.write!`
  before the code under test runs are the "previous durable state".

  ## Use

      crash = Fathom.Test.CrashFS.start!(dir)
      :ok = Storage.atomic_write(Path.join(dir, "obj"), "new")
      :ok = Fathom.Test.CrashFS.crash!(crash)
      assert File.read!(Path.join(dir, "obj")) == "new"

  `start!/2` accepts `skip: [:sync_file | :sync_dir | :rename | :link]` (those operations are
  reported done and NOT performed: a stubbed-out fsync, the regression test's way of proving the
  double bites) and `inject: fn op -> :ok | {:error, reason} end` (fail an operation, e.g. one rename).
  Both can be changed later with `set/2`.

  ## Scoping

  The hook is installed once for the whole suite by `install!/0` (`test/test_helper.exs`), but a
  CrashFS only ever sees operations whose paths are under ITS directory; every other test's
  operations are answered `:ok` without touching it, so async tests never observe each other.
  """
  use GenServer

  @registry __MODULE__.Registry

  @type op ::
          {:rename, Path.t(), Path.t()}
          | {:link, Path.t(), Path.t()}
          | {:sync_file, Path.t()}
          | {:sync_dir, Path.t()}
          | {:wrote, Path.t()}

  # ── suite-wide installation ──────────────────────────────────────────────────────────────────

  @doc "Create the scope registry and install the `:fs_hook`. Called once from `test_helper.exs`."
  @spec install!() :: :ok
  def install! do
    if :ets.whereis(@registry) == :undefined do
      :ets.new(@registry, [:named_table, :public, :set, read_concurrency: true])
    end

    Application.put_env(:fathom, :fs_hook, &__MODULE__.hook/1)
  end

  @doc false
  @spec hook(op()) :: :ok | :skip | {:error, term()}
  def hook(op) do
    case scope(op) do
      nil ->
        :ok

      pid ->
        try do
          GenServer.call(pid, {:op, op}, 30_000)
        catch
          # The owning test is gone: nobody is observing, so behave like the real filesystem.
          :exit, _ -> :ok
        end
    end
  end

  defp scope(op) do
    if :ets.whereis(@registry) == :undefined or :ets.info(@registry, :size) == 0 do
      nil
    else
      paths = op_paths(op)

      Enum.find_value(:ets.tab2list(@registry), fn {dir, pid} ->
        if Enum.any?(paths, &under?(&1, dir)), do: pid
      end)
    end
  end

  defp op_paths({_kind, a, b}), do: [a, b]
  defp op_paths({_kind, a}), do: [a]

  defp under?(path, dir), do: path == dir or String.starts_with?(path, dir <> "/")

  # ── per-test instance ────────────────────────────────────────────────────────────────────────

  @doc "Start a CrashFS scoped to `dir` under the test supervisor. Returns its pid."
  @spec start!(Path.t(), keyword()) :: pid()
  def start!(dir, opts \\ []) do
    ExUnit.Callbacks.start_supervised!(
      Supervisor.child_spec({__MODULE__, Keyword.put(opts, :dir, dir)}, id: {__MODULE__, dir})
    )
  end

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Change `skip:` / `inject:` while the test runs."
  @spec set(pid(), keyword()) :: :ok
  def set(pid, opts), do: GenServer.call(pid, {:set, opts})

  @doc "Every operation requested so far, oldest first (including skipped and failed ones)."
  @spec trace(pid()) :: [op()]
  def trace(pid), do: GenServer.call(pid, :trace)

  @doc """
  Simulate a power loss: undo what was never made durable (see the moduledoc) and forget the
  journal, so what is left on disk is the new durable baseline. `renames: :keep` keeps the
  directory entries (only unsynced file DATA is lost). Returns `:ok`.
  """
  @spec crash!(pid(), keyword()) :: :ok
  def crash!(pid, opts \\ []),
    do: GenServer.call(pid, {:crash, Keyword.get(opts, :renames, :revert)})

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    dir = opts |> Keyword.fetch!(:dir) |> Path.expand()
    true = :ets.insert(@registry, {dir, self()})

    {:ok,
     %{
       dir: dir,
       skip: opts |> Keyword.get(:skip, []) |> MapSet.new(),
       inject: Keyword.get(opts, :inject, fn _ -> :ok end),
       trace: [],
       dirty: MapSet.new(),
       journal: []
     }}
  end

  @impl true
  def terminate(_reason, state) do
    :ets.delete(@registry, state.dir)
    :ok
  end

  @impl true
  def handle_call({:set, opts}, _from, state) do
    state =
      state
      |> then(&if(opts[:skip], do: %{&1 | skip: MapSet.new(opts[:skip])}, else: &1))
      |> then(&if(opts[:inject], do: %{&1 | inject: opts[:inject]}, else: &1))

    {:reply, :ok, state}
  end

  def handle_call(:trace, _from, state), do: {:reply, Enum.reverse(state.trace), state}

  def handle_call({:crash, renames}, _from, state) do
    dirty =
      case renames do
        :revert -> Enum.reduce(state.journal, state.dirty, &undo/2)
        :keep -> state.dirty
      end

    zero_fill(dirty)
    {:reply, :ok, %{state | journal: [], dirty: MapSet.new()}}
  end

  def handle_call({:op, op}, _from, state) do
    state = %{state | trace: [op | state.trace]}
    kind = elem(op, 0)

    cond do
      kind == :wrote ->
        {:reply, :ok, %{state | dirty: MapSet.put(state.dirty, elem(op, 1))}}

      kind in state.skip ->
        {:reply, :skip, state}

      true ->
        case state.inject.(op) do
          :ok -> {:reply, :ok, apply_op(op, state)}
          {:error, _} = err -> {:reply, err, state}
        end
    end
  end

  # Called BEFORE the real operation runs, so a rename can snapshot what it is about to replace.
  defp apply_op({:sync_file, path}, state), do: %{state | dirty: MapSet.delete(state.dirty, path)}

  defp apply_op({:sync_dir, dir}, state) do
    journal =
      Enum.map(state.journal, fn entry ->
        if Path.dirname(entry.dst) == dir or Path.dirname(entry.src) == dir,
          do: %{entry | durable?: true},
          else: entry
      end)

    %{state | journal: journal}
  end

  defp apply_op({:rename, src, dst}, state) do
    old =
      case File.read(dst) do
        {:ok, bytes} -> bytes
        {:error, _} -> nil
      end

    dirty = MapSet.delete(state.dirty, dst)

    dirty =
      if MapSet.member?(dirty, src), do: MapSet.put(MapSet.delete(dirty, src), dst), else: dirty

    entry = %{kind: :rename, src: src, dst: dst, old: old, durable?: false}
    %{state | dirty: dirty, journal: [entry | state.journal]}
  end

  defp apply_op({:link, src, dst}, state) do
    dirty =
      if MapSet.member?(state.dirty, src), do: MapSet.put(state.dirty, dst), else: state.dirty

    entry = %{kind: :link, src: src, dst: dst, old: nil, durable?: false}
    %{state | dirty: dirty, journal: [entry | state.journal]}
  end

  # The journal is kept newest-first and undone in that order (the last rename first, so a
  # destination replaced twice is restored through both snapshots). Returns the dirty set, with
  # a moved-back file's mark following it to its old name.
  defp undo(%{durable?: true}, dirty), do: dirty

  defp undo(%{kind: :rename, src: src, dst: dst, old: old}, dirty) do
    if File.exists?(dst) and not File.exists?(src) do
      File.rename!(dst, src)
      if old, do: File.write!(dst, old)

      if MapSet.member?(dirty, dst),
        do: dirty |> MapSet.delete(dst) |> MapSet.put(src),
        else: dirty
    else
      dirty
    end
  end

  defp undo(%{kind: :link, dst: dst}, dirty) do
    _ = File.rm(dst)
    MapSet.delete(dirty, dst)
  end

  defp zero_fill(dirty) do
    for path <- dirty, match?({:ok, %File.Stat{type: :regular}}, File.stat(path)) do
      File.write!(path, :binary.copy(<<0>>, File.stat!(path).size))
    end

    :ok
  end
end
