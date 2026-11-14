defmodule Fathom.Shard.Storage.ContractLocalTest do
  @moduledoc """
  `Fathom.Test.StorageContract` against the `Local` backend — see that module for why one shared
  suite runs against both (expert review 2026-08-01 #30, item 8).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.Local
  alias Fathom.Test.Env

  setup do
    dir = Path.join(System.tmp_dir!(), "contract_local_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    Env.put!(Local, dir: dir)

    on_exit(fn -> File.rm_rf(dir) end)

    :ok
  end

  use Fathom.Test.StorageContract, backend: Local
end
