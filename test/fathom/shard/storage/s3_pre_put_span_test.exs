defmodule Fathom.Shard.Storage.S3PrePutSpanTest do
  @moduledoc """
  The pre-PUT intent hook's duration is emitted as `[:fathom, :shard, :flush, :pre_put]` (expert
  review 2026-10-20-2027 #21): the measurement that decides whether overlapping the intent fsync
  with the body upload is worth building. Pins that the event fires once per hooked flush, with the
  hook's time in it, and not at all for an unhooked flush.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  setup do
    dir = Path.join(System.tmp_dir!(), "s3preput_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    prev = Application.get_env(:fathom, S3)
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> S3EtagStore.serve(conn, store) end
    )

    test = self()
    name = "pre-put-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      name,
      [:fathom, :shard, :flush, :pre_put],
      fn _, %{duration: d}, _, _ -> send(test, {:pre_put, d}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(name)
      File.rm_rf!(dir)

      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    local = Path.join(dir, "s.db")
    File.write!(local, :binary.copy("x", 10_000))
    %{local: local}
  end

  test "a hooked flush emits the hook's duration once", %{local: local} do
    hook = fn _md5 ->
      Process.sleep(20)
      :ok
    end

    assert {:ok, _, _} = S3.flush("pp1", local, nil, nil, nil, pre_put: hook)
    assert_receive {:pre_put, d}
    assert System.convert_time_unit(d, :native, :millisecond) >= 20
    refute_received {:pre_put, _}
  end

  test "an unhooked flush emits nothing", %{local: local} do
    assert {:ok, _, _} = S3.flush("pp2", local, nil)
    refute_received {:pre_put, _}
  end
end
