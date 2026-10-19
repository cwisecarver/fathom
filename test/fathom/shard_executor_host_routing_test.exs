defmodule Fathom.ShardExecutorHostRoutingTest do
  @moduledoc """
  Shard-isolation gate for `ShardExecutor.shard_from_conn/1`'s Host routing (AGENTS.md
  § Gates; expert review 2026-10-17 #27).

  #27 replaced the per-request split / all-labels / join / downcase with a once-normalised zone
  (`serving_zone/0`, memoised in :persistent_term) and a cut at the first dot. This is a ROUTING
  change, so the old implementation is kept below VERBATIM as an oracle and compared over a
  table of hosts x zones: for every pair the new function must return exactly the shard id (or
  refusal) the old one did. A drift would send one tenant's traffic to another's shard (or fail a
  tenant closed), so any difference is a release blocker.
  """
  use ExUnit.Case, async: false

  alias Fathom.ShardExecutor

  setup do
    keys = [:shard_base_domain, :allow_shard_override, :default_shard]
    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}

    Application.put_env(:fathom, :allow_shard_override, false)
    Application.delete_env(:fathom, :default_shard)

    on_exit(fn ->
      for {k, v} <- prev do
        if v == nil,
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end
    end)
  end

  defp resolve(host, zone) do
    if zone == nil,
      do: Application.delete_env(:fathom, :shard_base_domain),
      else: Application.put_env(:fathom, :shard_base_domain, zone)

    ShardExecutor.shard_from_conn(%Plug.Conn{host: host})
  end

  # ---- ORACLE: the pre-#27 implementation, copied verbatim (zone passed in, not read) --------

  defp old_resolve(host, zone) do
    (old_shard_from_host(host, zone) || nil) |> old_normalize()
  end

  defp old_normalize(nil), do: nil
  defp old_normalize(id) when is_binary(id), do: String.downcase(id, :ascii)

  defp old_shard_from_host(host, zone) do
    host = String.trim_trailing(host, ".")

    with false <- old_ip?(host),
         [sub | rest] when rest != [] <- String.split(host, "."),
         true <- Enum.all?([sub | rest], &(&1 != "")),
         true <- old_zone_matches?(rest, zone),
         true <- Fathom.ShardId.valid?(sub) do
      sub
    else
      _ -> nil
    end
  end

  defp old_zone_matches?(rest, zone) do
    case zone do
      nil ->
        true

      zone ->
        case zone |> String.trim() |> String.trim_leading(".") |> String.trim_trailing(".") do
          "" -> true
          normalized -> String.downcase(Enum.join(rest, ".")) == String.downcase(normalized)
        end
    end
  end

  defp old_ip?(host), do: host =~ ~r/^\d+\.\d+\.\d+\.\d+$/

  # ---- the table -------------------------------------------------------------------------------

  @zones [
    nil,
    "fathom.example",
    "Fathom.Example",
    "fathom.example.",
    ".fathom.example",
    "  fathom.example  ",
    "example",
    "a.b.c.d",
    "",
    "   ",
    "..",
    "a..b"
  ]

  @hosts [
    "a.fathom.example",
    "b.fathom.example",
    "A.FATHOM.EXAMPLE",
    "Acme.Fathom.Example",
    "acme.fathom.example.",
    "acme.fathom.example..",
    "acme.Fathom.EXAMPLE",
    "fathom.example",
    "fathom.example.",
    "example",
    "localhost",
    "localhost.",
    "a.localhost",
    "a.b.fathom.example",
    "x.a.fathom.example",
    "acme.fathom.example.evil.com",
    "evilfathom.example",
    "acme.xfathom.example",
    "acme.fathom.exampl",
    "acme.example",
    "acme.b.c.d",
    "a.a.b.c.d",
    "a.b",
    "a.b.",
    "10.0.0.1",
    "10.0.0.1.",
    "a.10.0.0.1",
    "999.1.1.1",
    # Digit-leading hosts that are NOT dotted quads: `ip?/1` now skips its regex for any host not
    # starting with a digit (expert review 2026-10-19 #22), so the digit-leading side must still
    # agree with the previous implementation.
    "42acme.fathom.example",
    "1.2.3.example",
    "1.2.3",
    "1.2.3.4.5",
    "1.2.3.4x",
    ".fathom.example",
    "..fathom.example",
    "acme..fathom.example",
    "acme.fathom..example",
    ".",
    "..",
    "",
    "a_b.fathom.example",
    "-a.fathom.example",
    "a-b.fathom.example",
    "ünï.fathom.example",
    "acme.fathom.exämple",
    "ACME.fathom.example",
    String.duplicate("a", 200) <> ".fathom.example",
    "a.a..b",
    "x.a..b"
  ]

  test "new Host routing returns exactly what the old implementation did, for every host x zone" do
    for zone <- @zones, host <- @hosts do
      assert resolve(host, zone) == old_resolve(host, zone),
             "host #{inspect(host)} zone #{inspect(zone)}: new=#{inspect(resolve(host, zone))} " <>
               "old=#{inspect(old_resolve(host, zone))}"
    end
  end

  test "the oracle table is not vacuous: it contains both resolutions and refusals" do
    results = for zone <- @zones, host <- @hosts, do: old_resolve(host, zone)
    assert Enum.any?(results, &is_binary/1)
    assert Enum.any?(results, &is_nil/1)
  end

  describe "pinned cases under a configured zone (cross-shard isolation)" do
    @zone "fathom.example"

    test "a.zone and b.zone resolve to a and b, never to each other" do
      assert resolve("a.fathom.example", @zone) == "a"
      assert resolve("b.fathom.example", @zone) == "b"
    end

    test "host case is folded" do
      assert resolve("ACME.Fathom.EXAMPLE", @zone) == "acme"
      assert resolve("Acme.fathom.example", @zone) == "acme"
    end

    test "a trailing dot (or several) does not change the shard" do
      assert resolve("acme.fathom.example.", @zone) == "acme"
      assert resolve("acme.fathom.example..", @zone) == "acme"
    end

    test "a host that merely ends with the zone text, or has extra labels, is refused" do
      assert resolve("acme.fathom.example.evil.com", @zone) == nil
      assert resolve("a.b.fathom.example", @zone) == nil
      assert resolve("acme.xfathom.example", @zone) == nil
      assert resolve("evilfathom.example", @zone) == nil
    end

    test "the zone itself and a bare label are not shards" do
      assert resolve("fathom.example", @zone) == nil
      assert resolve("example", @zone) == nil
      assert resolve("localhost", @zone) == nil
    end

    test "an IP host is not a shard" do
      assert resolve("10.0.0.1", @zone) == nil
      assert resolve("10.0.0.1", nil) == nil
    end

    test "a zone configured with different case / dots normalises to the same routing" do
      for z <- ["Fathom.Example", "fathom.example.", ".fathom.example", "  fathom.example "] do
        assert resolve("acme.fathom.example", z) == "acme", inspect(z)
        assert resolve("acme.other.example", z) == nil, inspect(z)
      end
    end

    test "changing the config value takes effect (the memo is keyed by the raw value)" do
      assert resolve("acme.one.example", "one.example") == "acme"
      assert resolve("acme.one.example", "two.example") == nil
      assert resolve("acme.one.example", "one.example") == "acme"
    end
  end
end
