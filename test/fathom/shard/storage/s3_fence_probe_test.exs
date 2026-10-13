defmodule Fathom.Shard.Storage.S3FenceProbeTest do
  # Expert review #16: every safety property in the system — lease mutual exclusion,
  # the flush fence, conditional release — is load-bearing on the store ENFORCING
  # conditional writes (412 on a failed If-Match / If-None-Match PUT). A store that
  # ignores the headers returns 200, so two concurrent acquirers both "win" and every
  # fenced flush "succeeds": silent split-brain with zero signal until data is lost.
  # Round-2 #4 extends the probe: the steal-time touch must also produce a GENUINELY
  # new etag (both alternating copy forms), else the zombie fence is a silent no-op.
  # The invariant: the boot probe passes on an enforcing+rotating store and raises
  # (refusing the boot) on a lax, non-rotating, or unreachable one.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3
  alias Fathom.Test.S3EtagStore

  defp put_s3_config(plug) do
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

  # A lax store: accepts every PUT, silently ignoring the conditional headers.
  defp lax_store(conn),
    do: Plug.Conn.send_resp(conn, if(conn.method == "DELETE", do: 204, else: 200), "")

  # Enforces conditional writes but its touch NEVER rotates the etag — the round-2
  # #4 hazard class (an exotic store where copies of identical bytes always etag
  # identically, in every form).
  defp non_rotating_store(conn) do
    q = conn.query_string || ""
    same = ~s("same-forever")

    cond do
      conn.method == "DELETE" ->
        Plug.Conn.send_resp(conn, 204, "")

      conn.method == "HEAD" ->
        conn |> Plug.Conn.put_resp_header("etag", same) |> Plug.Conn.send_resp(200, "")

      conn.method == "POST" and q =~ "uploads" ->
        Plug.Conn.send_resp(
          conn,
          200,
          "<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>"
        )

      conn.method == "POST" and q =~ "uploadId" ->
        Plug.Conn.send_resp(
          conn,
          200,
          "<CompleteMultipartUploadResult><ETag>&quot;same-forever&quot;</ETag></CompleteMultipartUploadResult>"
        )

      conn.method == "PUT" and q =~ "uploadId" ->
        Plug.Conn.send_resp(
          conn,
          200,
          "<CopyPartResult><ETag>&quot;same-forever&quot;</ETag></CopyPartResult>"
        )

      Plug.Conn.get_req_header(conn, "if-match") not in [[], [same]] ->
        Plug.Conn.send_resp(conn, 412, "")

      Plug.Conn.get_req_header(conn, "if-none-match") == ["*"] ->
        Plug.Conn.send_resp(conn, 412, "")

      true ->
        conn |> Plug.Conn.put_resp_header("etag", same) |> Plug.Conn.send_resp(200, "")
    end
  end

  test "passes against a store that enforces conditional writes and rotates the touch" do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})
    put_s3_config(fn conn -> S3EtagStore.serve(conn, store) end)

    assert S3.verify_conditional_writes!() == :ok
  end

  test "refuses to boot against a store that ignores conditional writes" do
    put_s3_config(&lax_store/1)

    assert_raise RuntimeError, ~r/does not enforce/, fn ->
      S3.verify_conditional_writes!()
    end
  end

  # Round-2 #4: an enforcing store whose touch does not rotate the etag would make
  # the zombie fence a silent no-op — the boot probe must catch it.
  test "refuses to boot against a store whose touch does not rotate the etag" do
    put_s3_config(&non_rotating_store/1)

    assert_raise RuntimeError, ~r/did not rotate/, fn ->
      S3.verify_conditional_writes!()
    end
  end

  # Expert review 2026-10-12 N1: a touch that MOVES the etag is not enough if a second touch
  # moves it back — two steals then re-arm a zombie's If-Match on the first etag. This store
  # enforces every conditional and rotates on every write, but its etag only ever alternates
  # between two values (as the old copy-form touch did for identical bytes on a real store).
  defp two_cycle_store(conn, agent) do
    etag = fn n -> ~s("cycle-#{rem(n, 2)}") end
    %{exists: exists?, writes: n} = Agent.get(agent, & &1)
    if_match = Plug.Conn.get_req_header(conn, "if-match")
    if_none_match = Plug.Conn.get_req_header(conn, "if-none-match")
    matches? = exists? and if_match in [[], [etag.(n)]]

    case conn.method do
      "HEAD" when exists? ->
        conn
        |> Plug.Conn.put_resp_header("etag", etag.(n))
        |> Plug.Conn.put_resp_header("content-length", "18")
        |> Plug.Conn.send_resp(200, "")

      "GET" when matches? ->
        conn
        |> Plug.Conn.put_resp_header("etag", etag.(n))
        |> Plug.Conn.send_resp(200, "fathom-fence-probe")

      "PUT" when if_none_match == ["*"] and exists? ->
        Plug.Conn.send_resp(conn, 412, "")

      "PUT" when if_match == [] or matches? ->
        Agent.update(agent, &%{&1 | exists: true, writes: &1.writes + 1})
        conn |> Plug.Conn.put_resp_header("etag", etag.(n + 1)) |> Plug.Conn.send_resp(200, "")

      "DELETE" when matches? ->
        Agent.update(agent, &%{&1 | exists: false})
        Plug.Conn.send_resp(conn, 204, "")

      "HEAD" ->
        Plug.Conn.send_resp(conn, 404, "")

      _ ->
        Plug.Conn.send_resp(conn, 412, "")
    end
  end

  test "refuses to boot against a store whose second touch returns to the first etag (N1)" do
    agent = start_supervised!({Agent, fn -> %{exists: false, writes: 0} end})
    put_s3_config(&two_cycle_store(&1, agent))

    assert_raise RuntimeError, ~r/two NEW etags/, fn ->
      S3.verify_conditional_writes!()
    end
  end

  test "fails closed when the store is unreachable" do
    put_s3_config(fn _conn -> raise "connection refused" end)

    assert_raise RuntimeError, fn -> S3.verify_conditional_writes!() end
  end

  # Expert review 2026-09-05 #8: the release fence is a conditional DELETE, and a store that
  # enforces conditional PUTs but IGNORES If-Match on DELETE (returns 204 for a stale-etag release)
  # is a silent single-writer breach — a zombie's release removes a live owner's lock. This is the
  # case no prior probe or S3-path test could see: the old boot self-test never issued a
  # conditional DELETE, and S3EtagStore.delete_object used to delete unconditionally too. The boot
  # probe must now refuse it.
  test "refuses to boot against a store that ignores conditional DELETE (finding #8)" do
    store = start_supervised!({Agent, fn -> S3EtagStore.initial(%{}) end})

    # Enforces every PUT/copy conditional and rotates the touch (delegates to the faithful double),
    # but a bare object DELETE ignores If-Match and always 204s.
    put_s3_config(fn conn ->
      if conn.method == "DELETE" and not (conn.query_string =~ "uploadId") do
        Plug.Conn.send_resp(conn, 204, "")
      else
        S3EtagStore.serve(conn, store)
      end
    end)

    assert_raise RuntimeError, ~r/conditional DELETE/, fn ->
      S3.verify_conditional_writes!()
    end
  end
end
