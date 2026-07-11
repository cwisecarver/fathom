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

  test "percentile_cumulative is 0.0 for an empty or zero histogram" do
    assert PrometheusScrape.percentile_cumulative([], 99) == 0.0
    assert PrometheusScrape.percentile_cumulative([{1.0, 0.0}, {:infinity, 0.0}], 99) == 0.0
  end

  test "diff_buckets subtracts a previous scrape into a windowed cumulative histogram" do
    prev = [{1.0, 1.0}, {5.0, 3.0}, {:infinity, 4.0}]
    cur = [{1.0, 2.0}, {5.0, 8.0}, {:infinity, 10.0}]
    assert PrometheusScrape.diff_buckets(cur, prev) == [{1.0, 1.0}, {5.0, 5.0}, {:infinity, 6.0}]
  end
end
