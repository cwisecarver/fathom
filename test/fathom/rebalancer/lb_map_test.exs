defmodule Fathom.Rebalancer.LbMapTest do
  @moduledoc "The nginx exception-map renderer — pure, deterministic."
  use ExUnit.Case, async: true

  alias Fathom.Rebalancer.LbMap

  @backends %{"fathom1" => "fathom1:8080", "fathom2" => "fathom2:8080"}

  defp override(shard, node), do: %{shard_id: shard, pinned_node: node}

  test "empty override set: pure-hash default plus the static pin-upstreams" do
    out = LbMap.render([], @backends, "fathom.test")

    assert out =~ "map $host $fathom_target {"
    assert out =~ "default fathom_hrana;"
    # No pinned hosts, but the pin-upstreams for every backend are still emitted.
    refute out =~ "fathom.test fathom_pin"
    assert out =~ "upstream fathom_pin_fathom1 {"
    assert out =~ "upstream fathom_pin_fathom2 {"
    assert out =~ "server fathom1:8080"
  end

  test "a pin renders a map entry to that node's pin-upstream" do
    out = LbMap.render([override("hot_1", "fathom2")], @backends, "fathom.test")

    assert out =~ "hot_1.fathom.test fathom_pin_fathom2;"
    assert out =~ "default fathom_hrana;"
  end

  test "each pin upstream lists other backends as `backup` (dead-pin failover, #1)" do
    # Regression for #1: a single-server pin upstream turns a pinned shard's node death
    # into an indefinite 502 (no other server). With backups, nginx fails the pin over to
    # a survivor, which steals the stale lease — restoring the self-healing a non-pinned
    # shard gets from the hash pool.
    out = LbMap.render([], @backends, "fathom.test")

    # fathom1's pin: fathom1 primary, fathom2 as backup.
    assert out =~
             ~r/upstream fathom_pin_fathom1 \{\n    server fathom1:8080 max_fails=2 fail_timeout=10s;\n    server fathom2:8080 backup;\n    keepalive 16;\n\}/

    # fathom2's pin: fathom2 primary, fathom1 as backup.
    assert out =~
             ~r/upstream fathom_pin_fathom2 \{\n    server fathom2:8080 max_fails=2 fail_timeout=10s;\n    server fathom1:8080 backup;\n    keepalive 16;\n\}/
  end

  test "a single-node fleet renders a pin upstream with no backup server" do
    # No other backend to fail over to; just the primary (matches pre-#1 behavior).
    out = LbMap.render([], %{"solo" => "solo:8080"}, "fathom.test")

    assert out =~
             "upstream fathom_pin_solo {\n    server solo:8080 max_fails=2 fail_timeout=10s;\n    keepalive 16;\n}"

    refute out =~ "backup;"
  end

  test "entries are shard-sorted so a re-render with the same pins is byte-identical" do
    a =
      LbMap.render(
        [override("hot_3", "fathom1"), override("hot_1", "fathom2")],
        @backends,
        "fathom.test"
      )

    b =
      LbMap.render(
        [override("hot_1", "fathom2"), override("hot_3", "fathom1")],
        @backends,
        "fathom.test"
      )

    assert a == b, "render must be order-independent (deterministic reload)"
    # hot_1 line precedes hot_3 line regardless of input order.
    assert :binary.match(a, "hot_1.fathom.test") < :binary.match(a, "hot_3.fathom.test")
  end

  test "a pin to an unknown backend is skipped (never point the map at a missing upstream)" do
    out = LbMap.render([override("hot_1", "ghost_node")], @backends, "fathom.test")

    refute out =~ "hot_1.fathom.test"
    refute out =~ "fathom_pin_ghost_node"
  end

  test "upstream_name sanitizes non-identifier chars (e.g. an IP node key)" do
    assert LbMap.upstream_name("fathom1") == "fathom_pin_fathom1"
    assert LbMap.upstream_name("10.0.0.12") == "fathom_pin_10_0_0_12"

    out =
      LbMap.render(
        [override("hot_1", "10.0.0.12")],
        %{"10.0.0.12" => "10.0.0.12:8080"},
        "acme.example"
      )

    assert out =~ "hot_1.acme.example fathom_pin_10_0_0_12;"
    assert out =~ "upstream fathom_pin_10_0_0_12 {"
  end
end
