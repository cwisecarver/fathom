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
