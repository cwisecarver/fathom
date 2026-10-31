defmodule Fathom.Admin.PrometheusScrapeTest do
  @moduledoc "Parsing + histogram math for the metrics-layer scrape reader (pure, deterministic)."
  use ExUnit.Case, async: true

  alias Fathom.Admin.PrometheusScrape

  @scrape """
  # HELP fathom_s3_op_count S3 operations by method
  # TYPE fathom_s3_op_count counter
  fathom_s3_op_count{op="get"} 10
  fathom_s3_op_count{op="put"} 4
  # TYPE fathom_node_memory_total_bytes gauge
  fathom_node_memory_total_bytes 1048576
  # TYPE fathom_shard_query_duration_milliseconds histogram
  fathom_shard_query_duration_milliseconds_bucket{le="1.0"} 2
  fathom_shard_query_duration_milliseconds_bucket{le="5.0"} 8
  fathom_shard_query_duration_milliseconds_bucket{le="10.0"} 10
  fathom_shard_query_duration_milliseconds_bucket{le="+Inf"} 10
  fathom_shard_query_duration_milliseconds_sum 33.0
  fathom_shard_query_duration_milliseconds_count 10
  """

  test "parse skips comments and reads name/labels/value" do
    samples = PrometheusScrape.parse(@scrape)
    assert {"fathom_s3_op_count", %{"op" => "get"}, 10.0} in samples
    assert Enum.any?(samples, fn {n, _, _} -> n == "fathom_node_memory_total_bytes" end)
    refute Enum.any?(samples, fn {n, _, _} -> String.starts_with?(n, "#") end)
  end

  test "value matches by prefix + labels and skips _bucket/_sum" do
    s = PrometheusScrape.parse(@scrape)
    assert PrometheusScrape.value(s, "fathom_s3_op_count", %{"op" => "get"}) == 10.0
    assert PrometheusScrape.value(s, "fathom_s3_op_count", %{"op" => "put"}) == 4.0
    # unit-suffixed gauge still matches on the dotted-name prefix
    assert PrometheusScrape.value(s, "fathom_node_memory_total") == 1_048_576.0
    # absent family → default
    assert PrometheusScrape.value(s, "fathom_nope", %{}, -1.0) == -1.0
    # the histogram _count is a scalar (not _bucket/_sum), so it's reachable by its own prefix
    assert PrometheusScrape.value(s, "fathom_shard_query_duration_milliseconds_count") == 10.0
  end

  test "label_values enumerates a family's label" do
    s = PrometheusScrape.parse(@scrape)

    assert Enum.sort(PrometheusScrape.label_values(s, "fathom_s3_op_count", "op")) == [
             "get",
             "put"
           ]
  end

  test "buckets returns sorted cumulative pairs with +Inf last" do
    s = PrometheusScrape.parse(@scrape)
    b = PrometheusScrape.buckets(s, "fathom_shard_query_duration")
    assert b == [{1.0, 2.0}, {5.0, 8.0}, {10.0, 10.0}, {:infinity, 10.0}]
  end

  test "percentile_cumulative interpolates within the crossing bucket" do
    b = [{1.0, 2.0}, {5.0, 8.0}, {10.0, 10.0}, {:infinity, 10.0}]

    # total=10; p50 → target 5 lands in bucket (1,5] where cum goes 2→8: 1 + (5-2)/(8-2)*(5-1) = 3.0
    assert_in_delta PrometheusScrape.percentile_cumulative(b, 50), 3.0, 0.001
    # p99 → target 9.9 lands in (5,10] where cum goes 8→10: 5 + (9.9-8)/(10-8)*(10-5) = 9.75
    assert_in_delta PrometheusScrape.percentile_cumulative(b, 99), 9.75, 0.001
  end

  # Invariant (2026-10-15): the query-duration histogram carries 150/750/2500 ms edges, so a
  # percentile that lands in one of those ranges interpolates inside a narrow bucket. Before the
  # 750 edge, a p99 of this shape interpolated across 500-1000 and read 950, not 750.
  test "percentile_cumulative resolves on the new 750 and 2500 ms edges" do
    # total=100, p99 → target 99. Bucket (500,750] holds 90→99, so the crossing is exactly 750.
    at_750 = [{500.0, 90.0}, {750.0, 99.0}, {1000.0, 100.0}, {:infinity, 100.0}]
    assert_in_delta PrometheusScrape.percentile_cumulative(at_750, 99), 750.0, 0.001

    # p99 with 90 below 1000 and 100 below 2500: 1000 + (99-90)/(100-90) * (2500-1000) = 2350.
    in_2500 = [{1000.0, 90.0}, {2500.0, 100.0}, {5000.0, 100.0}, {:infinity, 100.0}]
    assert_in_delta PrometheusScrape.percentile_cumulative(in_2500, 99), 2350.0, 0.001
  end

  test "percentile_cumulative is 0.0 for an empty or zero histogram" do
    assert PrometheusScrape.percentile_cumulative([], 99) == 0.0
    assert PrometheusScrape.percentile_cumulative([{1.0, 0.0}, {:infinity, 0.0}], 99) == 0.0
  end

  test "diff_buckets subtracts a previous scrape into a windowed cumulative histogram" do
    prev = [{1.0, 1.0}, {5.0, 3.0}, {:infinity, 4.0}]
    cur = [{1.0, 2.0}, {5.0, 8.0}, {:infinity, 10.0}]
    assert PrometheusScrape.diff_buckets(cur, prev) == [{1.0, 1.0}, {5.0, 5.0}, {:infinity, 6.0}]
  end

  # Regression (expert review 2026-07-14 #23): parse_labels split on "," then matched `[k, v]`,
  # which raised a MatchError on a label value containing a comma or an escaped quote — and that
  # crashed the whole collector tick (violating the moduledoc's "never a crash / yields zeros"
  # contract). Parsing must now be TOTAL: a malformed line degrades to a dropped/best-effort
  # sample, never a raise.
  test "a label value containing a comma does not crash the parser" do
    text = ~s(fathom_shard_query_duration{db="a,b"} 3\n)
    samples = PrometheusScrape.parse(text)

    # No raise; the line still yields a sample (best-effort labels are fine, the contract is no crash).
    assert [{"fathom_shard_query_duration", labels, 3.0}] = samples
    assert is_map(labels)
  end

  test "a label value containing an escaped quote does not crash the parser" do
    text = ~s(fathom_s3_op_count{op="a\\"b"} 7\n)
    samples = PrometheusScrape.parse(text)

    assert [{"fathom_s3_op_count", labels, 7.0}] = samples
    assert is_map(labels)
  end

  test "a label pair with no '=' is dropped, not raised" do
    text = "fathom_thing{bogus} 1\n"
    assert [{"fathom_thing", %{}, 1.0}] = PrometheusScrape.parse(text)
  end

  # expert review 2026-10-29 #12: `parse/1` gained a binary-pattern fast path for canonical lines.
  # It must be a pure speed-up: the same text yields the same samples, line for line, as the
  # regex/split parser it replaced. `legacy_parse/1` below IS that old implementation, kept here
  # verbatim as the oracle. NOTE this does not prove the fast path is taken (a parser that always
  # fell back would also pass); the speed is measured, not asserted (commit message).
  describe "parse/1 fast path equals the legacy regex parser (expert review 2026-10-29 #12)" do
    @legacy_line ~r/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(.+)$/

    defp legacy_parse(text) when is_binary(text) do
      text |> String.split("\n", trim: true) |> Enum.flat_map(&legacy_line/1)
    end

    defp legacy_line("#" <> _), do: []

    defp legacy_line(line) do
      case Regex.run(@legacy_line, String.trim(line)) do
        [_, name, labels, value] ->
          case legacy_value(value) do
            {:ok, v} -> [{name, legacy_labels(labels), v}]
            :error -> []
          end

        _ ->
          []
      end
    rescue
      _ -> []
    end

    defp legacy_value("+Inf"), do: {:ok, :infinity}
    defp legacy_value("-Inf"), do: {:ok, :neg_infinity}
    defp legacy_value("NaN"), do: :error

    defp legacy_value(v) do
      case Float.parse(v) do
        {f, _} -> {:ok, f}
        :error -> :error
      end
    end

    defp legacy_labels("{}"), do: %{}

    defp legacy_labels(braces) do
      braces
      |> String.trim_leading("{")
      |> String.trim_trailing("}")
      |> String.split(",", trim: true)
      |> Enum.flat_map(fn pair ->
        case String.split(pair, "=", parts: 2) do
          [k, v] -> [{String.trim(k), v |> String.trim() |> String.trim("\"")}]
          _ -> []
        end
      end)
      |> Map.new()
    end

    # Canonical lines, as the reporters print them, plus every shape the fast path must hand to the
    # slow parser (or reject the same way): whitespace variants, odd label values, odd numbers.
    @corpus [
      ~s(fathom_s3_op_count{op="get",status_class="2xx"} 10),
      ~s(fathom_shard_query_duration_milliseconds_bucket{le="0.25"} 2),
      ~s(fathom_shard_query_duration_milliseconds_bucket{le="+Inf"} 10),
      ~s(fathom_shard_query_duration_milliseconds_sum 33.0),
      ~s(fathom_shard_query_duration_milliseconds_sum 1.2345e-5),
      ~s(fathom_x_sum 1.0e20),
      ~s(fathom_x{a="1"} -3.5),
      ~s(fathom_x{a="1"} +Inf),
      ~s(fathom_x{a="1"} -Inf),
      ~s(fathom_x{a="1"} NaN),
      ~s(fathom_x{a="1"} nan),
      ~s(fathom_x{} 4),
      ~s(fathom_x{}  4),
      ~s(fathom_x{a="1",} 4),
      ~s(fathom_x{,a="1"} 4),
      ~s(fathom_x{a="1",a="2"} 4),
      ~s(fathom_x{a=""} 4),
      ~s(fathom_x{a=" b "} 4),
      ~s(fathom_x{a="b c"} 4),
      ~s(fathom_x{a="b,c"} 4),
      ~s(fathom_x{a="b=c"} 4),
      ~s(fathom_x{a="b}c"} 4),
      ~s(fathom_x{a="b{c"} 4),
      ~s(fathom_x{a="b\\"c"} 4),
      ~s(fathom_x{a="b\\\\c"} 4),
      ~s(fathom_x{a="b\\nc"} 4),
      ~s(fathom_x{a="é"} 4),
      ~s(fathom_x{a-b="c"} 4),
      ~s(fathom_x{ a="b"} 4),
      ~s(fathom_x{a ="b"} 4),
      ~s(fathom_x{a= "b"} 4),
      ~s(fathom_x{a='b'} 4),
      ~s(fathom_x{a=b} 4),
      ~s(fathom_x{bogus} 4),
      ~s(fathom_x{a="1"}4),
      ~s(fathom_x{a="1"}),
      ~s(fathom_x{a="1"} ),
      ~s(fathom_x{a="1"}  4),
      ~s(fathom_x{a="1"}\t4),
      ~s(fathom_x\t4),
      ~s(fathom_x  4),
      ~s(fathom_x 4 ),
      ~s(fathom_x 4\t),
      ~s(fathom_x 4\r),
      "fathom_x 4\u00A0",
      "\u00A0fathom_x 4",
      ~s( fathom_x 4),
      ~s(\tfathom_x 4),
      ~s(fathom_x 4 5),
      ~s(fathom_x 4x),
      ~s(fathom_x x4),
      ~s(fathom_x 1e),
      ~s(fathom_x --1),
      ~s(fathom_x .5),
      ~s(fathom_x 5.),
      ~s(fathom_x +5),
      ~s(fathom_x 1e5),
      ~s(fathom_x 0x10),
      ~s(fathom_x),
      ~s(fathom_x ),
      ~s(9fathom_x 4),
      ~s(:colon:name_1 4),
      ~s(_under 4),
      ~s(fathom-x 4),
      ~s(fathom.x 4),
      ~s(# HELP fathom_x some text 4),
      ~s(#fathom_x 4),
      ~s( # fathom_x 4),
      "",
      " ",
      "{}",
      ~s({a="b"} 4),
      ~s(fathom_x{a="1"} 4 # comment)
    ]

    test "every corpus line parses the same as the legacy parser, alone and in one text" do
      for line <- @corpus do
        assert PrometheusScrape.parse(line) == legacy_parse(line), "diverged on #{inspect(line)}"
      end

      text = Enum.join(@corpus, "\n")
      assert PrometheusScrape.parse(text) == legacy_parse(text)
      assert PrometheusScrape.parse(text <> "\n") == legacy_parse(text <> "\n")
    end

    test "a blank scrape stays [] (the collector's hold-the-baselines sentinel)" do
      for blank <- ["", "\n", "\n\n\n"] do
        assert PrometheusScrape.parse(blank) == []
        assert legacy_parse(blank) == []
      end

      assert PrometheusScrape.parse(:not_a_binary) == []
    end

    test "the whole @scrape fixture parses the same, and in the same order" do
      assert PrometheusScrape.parse(@scrape) == legacy_parse(@scrape)
      assert [_ | _] = PrometheusScrape.parse(@scrape)
    end

    # Seeded, so a failure reproduces: 30k lines drawn from the bytes the parser branches on, half
    # of them random soup and half a canonical line with one byte mutated.
    test "fuzz: random and mutated lines parse the same as the legacy parser" do
      :rand.seed(:exsss, {2026, 10, 29})
      alphabet = ~c(aZ_:{}=",\\ \t09.eE+-InfNa\r#é) ++ [0xA0, 0x85, ?\n]
      canonical = for l <- @corpus, String.starts_with?(l, "fathom_"), do: l

      soup = fn ->
        for _ <- 1..Enum.random(1..20), into: <<>>, do: <<Enum.random(alphabet)::utf8>>
      end

      mutate = fn line ->
        i = :rand.uniform(byte_size(line)) - 1
        <<pre::binary-size(^i), _::binary-size(1), rest::binary>> = line
        pre <> <<Enum.random(alphabet)::utf8>> <> rest
      end

      for n <- 1..30_000 do
        line = if rem(n, 2) == 0, do: soup.(), else: mutate.(Enum.random(canonical))
        assert PrometheusScrape.parse(line) == legacy_parse(line), "diverged on #{inspect(line)}"
      end
    end
  end

  describe "index (expert review 2026-10-15 #23)" do
    test "every lookup gives the same answer on the index as on the flat list" do
      samples = PrometheusScrape.parse(@scrape)
      idx = PrometheusScrape.index(samples)

      assert PrometheusScrape.value(idx, "fathom_s3_op_count", %{"op" => "put"}) == 4.0
      assert PrometheusScrape.value(idx, "fathom_node_memory_total") == 1_048_576.0

      assert PrometheusScrape.buckets(idx, "fathom_shard_query_duration") ==
               PrometheusScrape.buckets(samples, "fathom_shard_query_duration")

      assert Enum.sort(PrometheusScrape.label_values(idx, "fathom_s3_op_count", "op")) ==
               ["get", "put"]

      # idempotent: indexing an index is a no-op
      assert PrometheusScrape.index(idx) == idx
    end

    test "an exact family name wins over a longer one sharing the prefix" do
      idx = PrometheusScrape.index(PrometheusScrape.parse("a_b_total 1\na_b 2\n"))
      assert PrometheusScrape.value(idx, "a_b") == 2.0
    end

    # Expert review 2026-10-17 #15: families/2 used to Map.keys + filter + sort ~200 names per
    # lookup. The index now carries the names sorted once; prefix resolution must be unchanged
    # (exact wins, then shorter-before-longer by name order) across near-collision names.
    test "prefix resolution over the pre-sorted names is unchanged" do
      text = "a_b_total 1\na_b 2\na_bc 3\na_b_count 4\nb_a 5\n"
      idx = PrometheusScrape.index(PrometheusScrape.parse(text))

      assert idx.names == Enum.sort(idx.names)
      assert PrometheusScrape.value(idx, "a_b") == 2.0
      # no exact "a_b_": the first by name order is a_b_count
      assert PrometheusScrape.value(idx, "a_b_") == 4.0
      assert PrometheusScrape.value(idx, "a_bc") == 3.0
      assert PrometheusScrape.value(idx, "zzz") == 0.0
      assert PrometheusScrape.label_values(idx, "a_b", "op") == []
    end

    test "sum/3 totals every tag value of a family" do
      text = ~s(fathom_held_count{aimed="true"} 3\nfathom_held_count{aimed="false"} 4\n)
      assert PrometheusScrape.sum(PrometheusScrape.parse(text), "fathom_held") == 7.0

      assert PrometheusScrape.sum(PrometheusScrape.parse(text), "fathom_held", %{
               "aimed" => "true"
             }) == 3.0

      assert PrometheusScrape.sum([], "fathom_held") == 0.0
    end
  end

  # A TAGGED histogram scraped without a filter produced one entry per tag value per `le`; walking
  # that as a single distribution gave nonsense percentiles (review 2026-10-15 #6).
  test "buckets/3 sums series across tags and can select one tag value" do
    text = """
    x_bucket{warm="true",le="1.0"} 4
    x_bucket{warm="true",le="+Inf"} 4
    x_bucket{warm="false",le="1.0"} 0
    x_bucket{warm="false",le="+Inf"} 2
    """

    s = PrometheusScrape.parse(text)
    assert PrometheusScrape.buckets(s, "x") == [{1.0, 4.0}, {:infinity, 6.0}]

    assert PrometheusScrape.buckets(s, "x", %{"warm" => "false"}) == [
             {1.0, 0.0},
             {:infinity, 2.0}
           ]

    assert PrometheusScrape.buckets(s, "x", %{"warm" => "nope"}) == []
  end
end
