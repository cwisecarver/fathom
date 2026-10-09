defmodule Fathom.Shard.Storage.S3TempCloseTest do
  @moduledoc """
  Expert review 2026-10-08 #26: the pull's download temp is written through a `delayed_write`
  buffer (db6cccc), so a write error (ENOSPC, EIO) surfaces at CLOSE. That close was a hard
  `:ok = File.close(fd)` — fail-closed, but as a raise: it skipped the whole-download retry and
  crashed a coordinator open that pulls outside a rescue. It must come back as an error result.

  The failure needs a write the kernel refuses at flush time, and `/dev/full` is the deterministic
  way to get one (every write is ENOSPC). Linux has it — CI runs this — macOS does not, so on a Mac
  the test is skipped and says so. Against the old `:ok = File.close(fd)` it raises `MatchError`.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Storage.S3

  if File.exists?("/dev/full") do
    test "a buffered write that fails at close is an error result, not a raise" do
      {:ok, fd} = File.open("/dev/full", [:write, :raw, :binary, {:delayed_write, 1_048_576, 50}])
      :ok = IO.binwrite(fd, :binary.copy(<<0>>, 4096))

      assert {:error, :enospc} = S3.close_download_temp(fd)
    end
  else
    @tag skip: "needs /dev/full (Linux); CI runs it"
    test "a buffered write that fails at close is an error result, not a raise" do
      :ok
    end
  end

  test "closing when nothing was ever opened is ok" do
    assert :ok = S3.close_download_temp(nil)
  end
end
