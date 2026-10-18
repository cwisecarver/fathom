defmodule Fathom.Shard.Storage.S3RecreateFlagTest do
  # Expert review 2026-10-17 #1: when the optimistic lock create 412s and the re-read finds the lock
  # GONE (its holder released in between), `acquire_existing/4` re-creates it. That lease must carry
  # `recreated: true` — the one signal the open has that a holder released mid-acquire, so its
  # pre-lease pull may hold the pre-final-flush object (`Shard.cold_fresh_check/5`). The first-try
  # create must NOT carry it: that path stays 1 RTT with no extra object read.
  # `ColdOpenReleaseRaceTest` drives the open side through `FaultyStorage`, which stamps the same flag.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

  defp stub!(plan) do
    agent = start_supervised!({Agent, fn -> plan end})
    test = self()

    plug = fn conn ->
      [{status, etag} | rest] = Agent.get(agent, & &1)
      Agent.update(agent, fn _ -> rest end)
      send(test, {:req, conn.method, status})

      conn
      |> then(fn c -> if etag, do: Plug.Conn.put_resp_header(c, "etag", etag), else: c end)
      |> Plug.Conn.send_resp(status, "")
    end

    prev = Application.get_env(:fathom, S3)

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: plug
    )

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)
  end

  test "a lock re-created after it vanished mid-acquire is marked recreated" do
    # create 412 (held) -> lock GET 404 (released in the gap) -> create 200.
    stub!([{412, nil}, {404, nil}, {200, "\"lock-v1\""}])
    assert {:ok, lease} = S3.acquire_lease("shard_x", "a@node#n", 30_000)
    assert lease.epoch == 1
    assert lease[:recreated] == true
    refute lease[:took_over]
    assert_received {:req, "PUT", 412}
    assert_received {:req, "GET", 404}
    assert_received {:req, "PUT", 200}
  end

  test "a first-try create is not marked (the uncontended open pays no re-check)" do
    stub!([{200, "\"lock-v1\""}])
    assert {:ok, lease} = S3.acquire_lease("shard_x", "a@node#n", 30_000)
    refute Map.has_key?(lease, :recreated)
  end
end
