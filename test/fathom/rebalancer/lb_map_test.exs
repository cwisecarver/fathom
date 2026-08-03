defmodule Fathom.Rebalancer.LbMapTest do
  @moduledoc "The nginx exception-map renderer — pure, deterministic."
  use ExUnit.Case, async: true

  alias Fathom.Rebalancer.LbMap

  @backends %{"fathom1" => "fathom1:8080", "fathom2" => "fathom2:8080"}

  defp override(shard, node), do: %{shard_id: shard, pinned_node: node}

  describe "the ROUTING property (#30 item 6)" do
    # Every test above asserts CONTENT — that some substring appears for a hand-written input.
    # None asserts the property the map exists to have, which is what actually keeps a tenant's
    # traffic on one node: a shard resolves to exactly one upstream, and it is its pin. A content
    # assertion cannot see a duplicate key (nginx silently takes the first, so a shard would route
    # to a stale node forever) or a pin that renders under the wrong host.
    #
    # Not a `StreamData` property — the interesting inputs here are structural (duplicates,
    # invalid ids, unknown backends, sort order), so they are enumerated directly.

    # Parse the rendered `map` block back into %{host => upstream}, failing loudly on a duplicate
    # key rather than silently keeping one — the duplicate is the bug this exists to catch.
    defp routing_table(out) do
      [_, block] = String.split(out, "map $host $fathom_target {", parts: 2)
      [block, _] = String.split(block, "\n}", parts: 2)

      block
      |> String.split("\n", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "#")))
      |> Enum.map(fn line -> line |> String.trim_trailing(";") |> String.split(~r/\s+/) end)
      |> Enum.reduce(%{}, fn [host, upstream], acc ->
        refute Map.has_key?(acc, host),
               "#{host} rendered TWICE — nginx keeps the first, so this shard routes to a stale " <>
                 "node until the duplicate is noticed"

        Map.put(acc, host, upstream)
      end)
    end

    test "every pinned shard resolves to exactly one upstream, and it is its pin" do
      pins = [
        override("alpha", "fathom1"),
        override("bravo", "fathom2"),
        override("charlie", "fathom1"),
        override("delta", "fathom2")
      ]

      table = routing_table(LbMap.render(pins, @backends, "fathom.test"))

      for %{shard_id: id, pinned_node: node} <- pins do
        assert table["#{id}.fathom.test"] == "fathom_pin_#{node}",
               "#{id} does not route to its pinned node #{node}"
      end

      # Plus the catch-all, and nothing else: an entry nobody asked for is a misroute.
      assert table["default"] == "fathom_hrana"
      assert map_size(table) == length(pins) + 1
    end

    test "the renderer does NOT dedupe — uniqueness is the unique index's job, not its own" do
      # Written first as "a shard pinned twice renders one entry", which FAILED: `render/3` emits
      # both, and nginx keeps the first, so the shard would route to a stale node.
      #
      # That is not a bug, because the input is impossible: `shard_overrides` carries
      # `unique_index(:shard_overrides, [:shard_id])`
      # (priv/repo/migrations/20260707030520_create_shard_overrides_and_node_key.exs:25), so the
      # rebalancer cannot produce two rows for one shard. The fixture was unrealistic — case 2 of
      # AGENTS.md's "an existing test that blocks your fix may be right".
      #
      # Pinned as the renderer's DEPENDENCY rather than deleted: this is the one place the map's
      # one-entry-per-shard property comes from somewhere else. Drop that index and the map
      # silently misroutes, with nothing in this file to catch it.
      out =
        LbMap.render(
          [override("alpha", "fathom1"), override("alpha", "fathom2")],
          @backends,
          "fathom.test"
        )

      assert out =~ "alpha.fathom.test fathom_pin_fathom1;"
      assert out =~ "alpha.fathom.test fathom_pin_fathom2;"
    end

    test "unroutable rows contribute NO entry rather than a broken one" do
      # An unknown backend, an invalid shard id, and a failed row: each must vanish, not render a
      # key pointing at an upstream that does not exist (nginx refuses to load the whole config).
      table =
        routing_table(
          LbMap.render(
            [
              override("good", "fathom1"),
              override("orphan", "fathom_gone"),
              override("bad id", "fathom1"),
              Map.put(override("reverted", "fathom2"), :failed_at, DateTime.utc_now())
            ],
            @backends,
            "fathom.test"
          )
        )

      assert table["good.fathom.test"] == "fathom_pin_fathom1"
      assert map_size(table) == 2, "only the routable pin and the default: #{inspect(table)}"
    end

    test "every upstream a map entry names is actually defined in the same render" do
      # The property that makes the config LOADABLE: a dangling upstream reference is a hard
      # nginx start failure, which takes the whole LB down rather than misrouting one tenant.
      out =
        LbMap.render(
          [override("alpha", "fathom1"), override("bravo", "fathom2")],
          @backends,
          "fathom.test"
        )

      for {_host, upstream} <- routing_table(out), upstream != "fathom_hrana" do
        assert out =~ "upstream #{upstream} {",
               "the map points at #{upstream}, which this render never defines — nginx would " <>
                 "refuse to load the config"
      end
    end
  end

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

  test "a pin upstream has exactly ONE backup, so a dead pin fails over deterministically (#17)" do
    # Expert review 2026-08-01 #17. This upstream carries no `hash $host consistent` (the main
    # pool does), so nginx load-balances multiple `backup` servers ROUND-ROBIN. With every
    # other backend listed, successive requests for ONE pinned host alternated between
    # survivors: both cold-open the shard, one wins acquire_lease, the other gets {:held,
    # winner} against a fresh heartbeat and errors immediately. On a 3-node fleet that is
    # roughly half of a pinned shard's requests failing for the whole outage — and pinned
    # shards are by definition the hottest tenants.
    three = %{
      "fathom1" => "fathom1:8080",
      "fathom2" => "fathom2:8080",
      "fathom3" => "fathom3:8080"
    }

    out = LbMap.render([], three, "fathom.test")

    for node <- ~w(fathom1 fathom2 fathom3) do
      block = upstream_block(out, "fathom_pin_#{node}")

      assert length(Regex.scan(~r/ backup;/, block)) == 1,
             "#{node}'s pin upstream must have exactly one backup, got:\n#{block}"
    end
  end

  test "the deterministic backup is stable across re-renders and spreads across survivors (#17)" do
    three = %{
      "fathom1" => "fathom1:8080",
      "fathom2" => "fathom2:8080",
      "fathom3" => "fathom3:8080"
    }

    a = LbMap.render([], three, "fathom.test")
    b = LbMap.render([], three, "fathom.test")
    assert a == b, "the render must stay byte-identical — LbApply's no-op check depends on it"

    backups =
      for node <- ~w(fathom1 fathom2 fathom3) do
        [_, addr] = Regex.run(~r/server (\S+) backup;/, upstream_block(a, "fathom_pin_#{node}"))
        addr
      end

    assert length(Enum.uniq(backups)) > 1,
           "every pin failing over to the SAME survivor would concentrate the whole fleet's " <>
             "pinned load on one node: #{inspect(backups)}"
  end

  # The text of one `upstream <name> { ... }` block.
  defp upstream_block(rendered, name) do
    [block] = Regex.run(~r/upstream #{name} \{[^}]*\}/, rendered)
    block
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
