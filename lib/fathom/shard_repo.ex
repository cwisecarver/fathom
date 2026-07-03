defmodule Fathom.ShardRepo do
  @moduledoc """
  Ecto repo for Fathom's libSQL/Turso shard databases.

  Unlike `Fathom.Repo` (the Postgres orchestration store and web UI backend),
  this repo is **not** started in the application supervision tree. Each shard is
  a separate libSQL database opened on demand and bound to the calling process
  with `Ecto.Repo.put_dynamic_repo/1`:

      {:ok, pid} = Fathom.ShardRepo.start_shard("/data/shards/0007.db")
      Fathom.ShardRepo.put_dynamic_repo(pid)
      Fathom.ShardRepo.all(SomeSchema)

  Each shard is started with `name: nil`, yielding an anonymous instance
  addressed by pid, so many shards can run concurrently under this one repo
  module. Routing a key to its shard pid is the job of the (forthcoming) shard
  router/manager.
  """
  use Ecto.Repo,
    otp_app: :fathom,
    adapter: Ecto.Adapters.LibSql

  @doc """
  Opens a single local libSQL shard at `path`, returning `{:ok, pid}`.

  Pass the returned pid to `put_dynamic_repo/1` to direct queries at this shard.
  Extra `opts` are merged into the connection options (e.g. `:pool_size`).
  """
  def start_shard(path, opts \\ []) when is_binary(path) do
    opts
    |> Keyword.merge(name: nil, database: path)
    |> Keyword.put_new(:pool_size, 1)
    |> start_link()
  end
end
