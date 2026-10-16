defmodule Fathom.FailureTelemetryTest do
  @moduledoc """
  Failure-branch telemetry for the admin UI (expert review 2026-10-15 #3, #11, #22): query
  errors by bounded class, no-quorum by reason, Hrana auth rejections by classified reason (the
  wire response must stay ONE opaque 401), and the S3 op status_class tag. No tag is ever a
  shard id.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Fathom.{HranaAuth, ShardExecutor, Shards}
  alias Filo.Stmt

  defp attach(event) do
    id = "fail-tel-#{System.unique_integer([:positive])}"
    test = self()

    :telemetry.attach(
      id,
      event,
      fn _e, meas, meta, _ -> send(test, {:evt, event, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  describe "query error class" do
    setup do
      shard = "ft_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        for key <- [:query_timeout_ms, :query_max_rows], do: Application.delete_env(:fathom, key)
        Shards.drain(shard, 2_000)

        for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
            path <- Path.wildcard(Path.join(dir, "#{shard}*")),
            do: File.rm(path)
      end)

      attach([:fathom, :shard, :query, :error])
      {:ok, h} = ShardExecutor.open(shard)
      %{h: h}
    end

    test "sql_error", %{h: h} do
      assert {:error, %Filo.Error{code: "SQLITE_ERROR"}} =
               ShardExecutor.execute(h, stmt("SELEC nope"))

      assert_receive {:evt, _, %{count: 1}, %{class: :sql_error} = meta}
      assert Map.keys(meta) == [:class]
    end

    test "timeout", %{h: h} do
      Application.put_env(:fathom, :query_timeout_ms, 50)

      slow =
        "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x < 100000000) " <>
          "SELECT count(*) FROM r"

      assert {:error, %Filo.Error{code: "FILO_QUERY_TIMEOUT"}} =
               ShardExecutor.execute(h, stmt(slow))

      assert_receive {:evt, _, _, %{class: :timeout}}
    end

    test "too_large", %{h: h} do
      {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (v INTEGER)"))

      for i <- 1..5,
          do: {:ok, _} = ShardExecutor.execute(h, stmt("INSERT INTO t VALUES (?)", [i]))

      Application.put_env(:fathom, :query_max_rows, 2)

      assert {:error, %Filo.Error{code: "FILO_RESULT_TOO_LARGE"}} =
               ShardExecutor.execute(h, stmt("SELECT v FROM t"))

      assert_receive {:evt, _, _, %{class: :too_large}}
    end

    test "a successful statement emits no error event", %{h: h} do
      assert {:ok, _} = ShardExecutor.execute(h, stmt("SELECT 1"))
      refute_receive {:evt, _, _, _}, 50
    end
  end

  describe "no_quorum" do
    test "reason is classified and bounded; the client error is unchanged" do
      attach([:fathom, :replication, :no_quorum])

      for {raw, want} <- [
            {{:no_quorum, :impossible}, :impossible},
            {{:no_quorum, :catching_up}, :catching_up},
            {{:session_down, :timeout}, :timeout},
            {{:weird, "/data/path pid"}, :other}
          ] do
        assert %Filo.Error{code: "FILO_NO_QUORUM", status: 503} =
                 ShardExecutor.no_quorum_error("x", raw)

        assert_receive {:evt, _, %{count: 1}, %{reason: ^want} = meta}
        assert Map.keys(meta) == [:reason]
      end
    end
  end

  describe "hrana auth rejection" do
    setup do
      prev = Application.get_env(:fathom, :hrana_auth, :disabled)
      prev_age = Application.get_env(:fathom, :hrana_token_max_age, :infinity)
      Application.put_env(:fathom, :hrana_auth, :required)

      on_exit(fn ->
        Application.put_env(:fathom, :hrana_auth, prev)
        Application.put_env(:fathom, :hrana_token_max_age, prev_age)
      end)

      attach([:fathom, :hrana, :auth, :rejected])
      :ok
    end

    defp token!(shard, opts \\ []) do
      {:ok, t} = HranaAuth.token_for(shard, opts)
      t
    end

    test "allowed path still succeeds and emits nothing" do
      assert {:ok, {:rw, _}} = HranaAuth.authorize("acme", token!("acme"))
      refute_receive {:evt, _, _, _}, 50
    end

    test "each denial is classified, and every wire error is the same 401 code" do
      Application.put_env(:fathom, :hrana_token_max_age, 60)
      expired = token!("acme", signed_at: System.system_time(:second) - 3600)
      other_shard = token!("globex")
      revoked_shard = "ft-revoked-#{System.unique_integer([:positive])}"
      revoked = token!(revoked_shard)
      HranaAuth.Revocations.put(revoked_shard, 1_000_000, nil)

      cases = [
        {"acme", expired, :expired},
        {"acme", "garbage", :invalid},
        {"acme", nil, :missing},
        {"acme", other_shard, :wrong_shard},
        {revoked_shard, revoked, :revoked}
      ]

      errors =
        for {shard, tok, want} <- cases do
          assert {:error, %Filo.Error{status: 401, code: "AUTH_REQUIRED"} = e} =
                   HranaAuth.authorize(shard, tok)

          assert_receive {:evt, _, %{count: 1}, %{reason: ^want} = meta}, 500
          assert Map.keys(meta) == [:reason]
          e
        end

      # Opaque: every non-missing denial is byte-identical on the wire (unchanged by this work).
      opaque = errors |> Enum.reject(&(&1.message == "missing bearer token")) |> Enum.uniq()
      assert [%Filo.Error{message: "unauthorized", code: "AUTH_REQUIRED", status: 401}] = opaque
    end
  end

  describe "s3 meter" do
    test "status_class tag is bounded" do
      attach([:fathom, :s3, :op])

      for {status, want} <- [
            {200, "2xx"},
            {304, "3xx"},
            {404, "4xx"},
            {503, "5xx"},
            {100, "error"}
          ] do
        req = Req.new(method: :get)
        resp = %Req.Response{status: status}
        assert {^req, ^resp} = Fathom.Shard.Storage.S3.meter({req, resp})
        assert_receive {:evt, _, %{count: 1}, %{op: :get, status_class: ^want}}
      end
    end
  end
end
