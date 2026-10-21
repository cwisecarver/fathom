defmodule Fathom.FS do
  @moduledoc """
  Raw (`[:raw]`) file operations that bypass the node's single `file_server_2` process.

  A plain `File.*` call is one message to `file_server_2` (OTP's `file` module routes every
  non-raw call through it), so every shard cold-open, drop and replication reset serialized on one
  process node-wide (expert review 2026-10-21 #4). The raw variants do the syscall in the calling
  process. Semantics match `File.*` for the absolute paths fathom uses; return shapes are
  identical (`File.Stat` struct from `stat/1,2`).

  Not wrapped: `mkdir_p`, `ls`, `cp`, `rm_rf` (no raw variant, and not on a per-open hot path).
  """

  @spec exists?(Path.t()) :: boolean()
  def exists?(path), do: type(path) != nil

  @spec dir?(Path.t()) :: boolean()
  def dir?(path), do: type(path) == :directory

  @spec regular?(Path.t()) :: boolean()
  def regular?(path), do: type(path) == :regular

  # NB: `File.exists?(path, raw: true)` does NOT bypass the server: Elixir passes `{:raw, true}`
  # to `:file`, which only understands the bare atom `:raw` (probed: still one file_server_2 request).
  defp type(path) do
    case :file.read_file_info(IO.chardata_to_string(path), [:raw]) do
      {:ok, info} -> elem(info, 2)
      {:error, _} -> nil
    end
  end

  @spec stat(Path.t(), [{:time, :posix | :universal | :local}]) ::
          {:ok, File.Stat.t()} | {:error, File.posix()}
  def stat(path, opts \\ []) do
    time = Keyword.get(opts, :time, :universal)

    case :file.read_file_info(IO.chardata_to_string(path), [:raw, {:time, time}]) do
      {:ok, info} -> {:ok, File.Stat.from_record(info)}
      {:error, _} = err -> err
    end
  end

  @spec rm(Path.t()) :: :ok | {:error, File.posix()}
  def rm(path), do: :file.delete(IO.chardata_to_string(path), [:raw])

  @spec rename(Path.t(), Path.t()) :: :ok | {:error, File.posix()}
  def rename(src, dst),
    do: :prim_file.rename(IO.chardata_to_string(src), IO.chardata_to_string(dst))

  @spec read(Path.t()) :: {:ok, binary()} | {:error, File.posix()}
  def read(path), do: :file.read_file(IO.chardata_to_string(path), [:raw])

  @spec write(Path.t(), iodata()) :: :ok | {:error, File.posix()}
  def write(path, data), do: :file.write_file(IO.chardata_to_string(path), data, [:raw])
end
