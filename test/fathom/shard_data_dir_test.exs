defmodule Fathom.ShardDataDirTest do
  @moduledoc """
  expert review 2026-10-17 #3: `Shard.data_dir/0` (hence `db_path/1`, called on every replicated
  write) evaluated `System.tmp_dir!()` eagerly as the `get_env` default: ~16 us per call even when
  the directory is configured. Pins the lazy default (order-of-magnitude ceiling, not exact).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard

  setup do
    prior = Application.fetch_env(:fathom, :shard_data_dir)

    on_exit(fn ->
      case prior do
        {:ok, v} -> Application.put_env(:fathom, :shard_data_dir, v)
        :error -> Application.delete_env(:fathom, :shard_data_dir)
      end
    end)
  end

  test "configured dir wins and db_path joins under it" do
    Application.put_env(:fathom, :shard_data_dir, "/cfg/dir")
    assert Shard.data_dir() == "/cfg/dir"
    assert Shard.db_path("abc") == "/cfg/dir/abc.db"
  end

  test "unset falls back to the tmp_dir default" do
    Application.delete_env(:fathom, :shard_data_dir)
    assert Shard.data_dir() == Path.join(System.tmp_dir!(), "fathom_shards")
  end

  test "a configured dir does no tmp_dir work per call (eager default cost ~16 us)" do
    Application.put_env(:fathom, :shard_data_dir, "/cfg/dir")
    n = 20_000
    {us, _} = :timer.tc(fn -> for _ <- 1..n, do: Shard.db_path("x") end)
    assert us / n < 4.0, "db_path/1 averaged #{us / n} us; the tmp_dir default is eager again"
  end
end
