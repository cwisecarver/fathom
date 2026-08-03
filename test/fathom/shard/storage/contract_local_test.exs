defmodule Fathom.Shard.Storage.ContractLocalTest do
  @moduledoc """
  `Fathom.Test.StorageContract` against the `Local` backend — see that module for why one shared
  suite runs against both (expert review 2026-08-01 #30, item 8).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.Local

  setup do
    dir = Path.join(System.tmp_dir!(), "contract_local_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:fathom, Local)
    File.mkdir_p!(dir)
    Application.put_env(:fathom, Local, dir: dir)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, Local, prev),
        else: Application.delete_env(:fathom, Local)

      File.rm_rf(dir)
    end)

    :ok
  end

  use Fathom.Test.StorageContract, backend: Local
end
