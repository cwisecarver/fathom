defmodule Fathom.HealthPlugTest do
  # Pure plug test: no DB, no port bound — calls the plug function directly.
  use ExUnit.Case, async: true

  import Plug.Test
  import Plug.Conn

  defp call(method, path) do
    conn(method, path) |> Fathom.HealthPlug.call(Fathom.HealthPlug.init([]))
  end

  test "GET /health returns 200 ok (LB liveness probe target)" do
    conn = call(:get, "/health")
    assert conn.status == 200
    assert conn.resp_body == "ok"
    assert get_resp_header(conn, "content-type") |> List.first() =~ "text/plain"
  end

  test "an unknown path returns 404 (not a catch-all 200)" do
    conn = call(:get, "/nope")
    assert conn.status == 404
  end

  test "a non-GET to /health returns 404 (liveness is GET-only)" do
    conn = call(:post, "/health")
    assert conn.status == 404
  end

  # The admin Nodes table shows probe counts + the last non-200 (expert review 2026-10-15 #7). Deltas,
  # not absolutes: the counters are node-global and other tests flip the draining flag.
  describe "probe_stats/0" do
    test "counts probes and stamps the last non-200 when draining" do
      on_exit(fn -> Fathom.HealthPlug.end_draining() end)
      before = Fathom.HealthPlug.probe_stats()

      Fathom.HealthPlug.end_draining()
      assert call(:get, "/health").status == 200
      Fathom.HealthPlug.begin_draining()
      assert Fathom.HealthPlug.draining?()
      assert call(:get, "/health").status == 503

      after_ = Fathom.HealthPlug.probe_stats()
      assert after_.probes >= before.probes + 2
      assert after_.non_200 >= before.non_200 + 1
      assert %DateTime{} = after_.last_non_200_at
    end
  end
end
