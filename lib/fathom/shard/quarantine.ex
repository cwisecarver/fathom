defmodule Fathom.Shard.Quarantine do
  @moduledoc """
  The one way a local shard copy is moved aside as a quarantine (`.forked`, `.fenced`, `.corrupt`).

  Expert review 2026-11-07 #83 found `Fork.quarantine_copy!` renamed the `.db` FIRST and the
  `-wal`/`-shm` best-effort afterwards: a failed `-wal` rename, or a crash between the two, left the
  acked frames beside nothing at the live path, where the next open's fresh `.db` would REPLAY them
  (or `promote_pull` would drop them as stale). The same shape lived in `Shard.rename_fenced!/1` and
  `Integrity.quarantine_corrupt!/3`; all three now go through `move_aside/2`.

  Order and failure semantics:

    * the companions (`-wal`, then `-shm`) move BEFORE the `.db`, so every crash window leaves the
      `.db` at its live path (and at worst an inert `-wal` beside the quarantine name: SQLite never
      needs a `-wal` without its `.db`);
    * every result is checked; a companion that does not exist (`:enoent`: no `-shm` is normal, a
      cleanly closed WAL has no `-wal`) is simply absent;
    * on ANY failure everything already moved goes back, so the outcome is all-moved or nothing-moved.

  All renames go through `Fathom.FS.rename/2`, the crash-test seam (`Fathom.Test.CrashFS`).
  """
  require Logger

  @doc """
  Moves `path` and its `-wal`/`-shm` to `dest` and `dest <> suffix`, companions first. `:ok` when
  all moved; `{:error, reason}` with nothing moved (a rollback that itself fails is logged and the
  file is left at its quarantine name, where it is preserved).
  """
  @spec move_aside(Path.t(), Path.t()) :: :ok | {:error, term()}
  def move_aside(path, dest) do
    with {:ok, moved} <- move_companions(path, dest, ["-wal", "-shm"], []) do
      case Fathom.FS.rename(path, dest) do
        :ok ->
          :ok

        {:error, _} = error ->
          roll_back(path, dest, moved)
          error
      end
    end
  end

  defp move_companions(_path, _dest, [], moved), do: {:ok, moved}

  defp move_companions(path, dest, [suffix | rest], moved) do
    case Fathom.FS.rename(path <> suffix, dest <> suffix) do
      :ok ->
        move_companions(path, dest, rest, [suffix | moved])

      {:error, :enoent} ->
        move_companions(path, dest, rest, moved)

      {:error, _} = error ->
        roll_back(path, dest, moved)
        error
    end
  end

  defp roll_back(path, dest, moved) do
    for suffix <- moved do
      case Fathom.FS.rename(dest <> suffix, path <> suffix) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.error(
            "quarantine rollback: could not move #{dest <> suffix} back to " <>
              "#{path <> suffix} (#{inspect(reason)}); the file is preserved at the former name"
          )
      end
    end

    :ok
  end
end
