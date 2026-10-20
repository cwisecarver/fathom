defmodule Fathom.Shard.Storage.S3PullRetryTest do
  @moduledoc """
  expert review 2026-10-20 #14: the cold-open pull never retried a 503 SlowDown (or a 500/502/504),
  so under store throttling every correlated cold open failed at once, and the client's retry then
  repeated the whole open. Pre-fix the first 503 was `{:error, {:s3_get_status, 503}}` straight away.

  Invariants pinned here:
    * a transient GET status is retried with a FRESH download, so a store that recovers within the
      attempt budget yields a successful pull (the object lands, its etag is returned);
    * the budget is bounded: a store that never recovers gives up after exactly 3 GETs
      (`@download_attempts`), and the error is the status, not a hang or a crash.

  Driven against the `S3EtagStore` double with a fault-injecting `req_plug` in front of it, so the
  GET counts are observed on the wire rather than inferred from the code.
  """
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  @body "pulled-after-throttle"
  @slowdown ~s(<?xml version="1.0"?><Error><Code>SlowDown</Code><Message>slow</Message></Error>)

  setup %{tmp_dir: tmp_dir} do
    shard = "retry_#{System.unique_integer([:positive])}"
    prev = Application.get_env(:fathom, S3)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    store =
      start_supervised!(%{
        id: :store,
        start: {Agent, :start_link, [fn -> S3EtagStore.initial(%{"#{shard}.db" => @body}) end]}
      })

    # `fails` GETs answer 503 SlowDown before the store serves normally; `gets` counts every GET
    # that reached the double, so the test can assert the exact attempt count.
    counter =
      start_supervised!(%{
        id: :counter,
        start: {Agent, :start_link, [fn -> %{fails: 0, gets: 0} end]}
      })

    %{shard: shard, store: store, counter: counter, path: Path.join(tmp_dir, "pulled.db")}
  end

  defp use_store!(store, counter) do
    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: fn conn -> serve(conn, store, counter) end
    )
  end

  defp serve(%Plug.Conn{method: "GET"} = conn, store, counter) do
    throttled? =
      Agent.get_and_update(counter, fn %{fails: f, gets: g} = s ->
        {f > 0, %{s | fails: max(f - 1, 0), gets: g + 1}}
      end)

    if throttled? do
      Plug.Conn.send_resp(conn, 503, @slowdown)
    else
      S3EtagStore.serve(conn, store)
    end
  end

  defp serve(conn, store, _counter), do: S3EtagStore.serve(conn, store)

  defp gets(counter), do: Agent.get(counter, & &1.gets)

  test "a 503 SlowDown that clears within the attempt budget is retried and the pull succeeds",
       %{shard: shard, store: store, counter: counter, path: path} do
    Agent.update(counter, &%{&1 | fails: 1})
    use_store!(store, counter)

    assert {:ok, etag} = S3.pull(shard, path)
    assert is_binary(etag)
    assert File.read!(path) == @body

    # One throttled GET, then the successful retry: exactly two requests.
    assert gets(counter) == 2
  end

  test "a store that never recovers gives up after the bounded attempt budget (3 GETs)",
       %{shard: shard, store: store, counter: counter, path: path} do
    Agent.update(counter, &%{&1 | fails: 1_000})
    use_store!(store, counter)

    assert {:error, {:s3_get_status, 503}} = S3.pull(shard, path)
    refute File.exists?(path), "a failed pull must not leave a database behind"

    assert gets(counter) == 3, "the retry budget is bounded at @download_attempts (3)"
  end
end
