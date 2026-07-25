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
             ~r/upstream fathom_pin_fathom1 \{\n    server fathom1:8080 max_fails=2 fail_timeout=10s;\n    server fathom2:8080 backup;\n    keepalive 512;\n    keepalive_timeout 30s;\n    keepalive_requests 100000;\n\}/

    # fathom2's pin: fathom2 primary, fathom1 as backup.
    assert out =~
             ~r/upstream fathom_pin_fathom2 \{\n    server fathom2:8080 max_fails=2 fail_timeout=10s;\n    server fathom1:8080 backup;\n    keepalive 512;\n    keepalive_timeout 30s;\n    keepalive_requests 100000;\n\}/
  end

  test "a single-node fleet renders a pin upstream with no backup server" do
    # No other backend to fail over to; just the primary (matches pre-#1 behavior).
    out = LbMap.render([], %{"solo" => "solo:8080"}, "fathom.test")

    assert out =~
             "upstream fathom_pin_solo {\n    server solo:8080 max_fails=2 fail_timeout=10s;\n    keepalive 512;\n    keepalive_timeout 30s;\n    keepalive_requests 100000;\n}"

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

  test "an entry with an invalid shard_id is skipped, never rendered raw (#14)" do
    # Defense-in-depth: even if a malformed id reached the table, the renderer must not emit
    # it as a raw map key (which would inject nginx directives).
    evil = override("evil; } server { deny all; } #", "fathom1")
    out = LbMap.render([evil, override("hot_1", "fathom2")], @backends, "fathom.test")

    refute out =~ "evil"
    refute out =~ "deny all"
    # The valid pin still renders.
    assert out =~ "hot_1.fathom.test fathom_pin_fathom2;"
  end

  test "a failed/reverted override is skipped so traffic returns to the source (#4)" do
    # A row with failed_at set is a cooldown record only — it must not render a map entry
    # (that would keep routing to the target it failed to move to).
    failed = Map.put(override("hot_1", "fathom2"), :failed_at, ~U[2026-07-07 00:00:00Z])
    out = LbMap.render([failed], @backends, "fathom.test")

    refute out =~ "hot_1.fathom.test"
    assert out =~ "default fathom_hrana;"
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

  # Expert review 2026-07-24 #7: the pin upstream shipped `keepalive 16` against the main config's
  # 64 — so the moment Policy identified a shard as the hottest thing on the fleet and pinned it,
  # that tenant's upstream connection reuse was cut 4x and it began paying a fresh handshake per
  # evicted connection. Exactly backwards. A pin serves the hot minority and must never be sized
  # below the general hash pool.
  test "a pin upstream is not sized below the general hash pool (#7)" do
    out = LbMap.render([], %{"n1" => "n1:8080"}, "acme.example")

    [_, pool] = Regex.run(~r/keepalive (\d+);/, out)

    assert String.to_integer(pool) >= 512,
           "the pin pool (#{pool}) must be at least the main config's `keepalive 512` — a pinned " <>
             "shard is by definition one of the hottest on the fleet"

    assert out =~ "keepalive_requests 100000;",
           "without this nginx recycles a pooled conn every 1000 requests, re-opening the FIN " <>
             "race that keepalive_timeout closed only for the idle case"
  end
end
