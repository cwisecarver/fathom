defmodule Fathom.Shard.Storage.S3PartCopyEtagTest do
  # Chaos-rig boot failure (2026-07-05): the fence self-test's part-copy step parsed
  # the CopyPartResult ETag with a regex accepting only &quot; or a literal quote.
  # MinIO escapes the quote as the NUMERIC entity &#34;, so a store whose conditional
  # writes are perfectly fine failed the probe and the node refused to boot
  # ({:s3_part_copy_no_etag, ...}). The invariant: the parser accepts every legal XML
  # escaping of the quote — &quot; (AWS), &#34; (MinIO), and a bare " — because a
  # false negative here bricks boot against a known-good store.
  use ExUnit.Case, async: true

  alias Fathom.Shard.Storage.S3

  @etag "326f9aa51e7e40ad8a31bce7dacbf373"

  defp body(quoted_etag) do
    ~s(<?xml version="1.0" encoding="UTF-8"?>\n) <>
      ~s(<CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">) <>
      ~s(<LastModified>2026-07-06T01:30:58.456Z</LastModified>) <>
      ~s(<ETag>#{quoted_etag}</ETag></CopyPartResult>)
  end

  test "AWS-style &quot; escaping parses" do
    assert {:ok, @etag} = S3.copy_part_etag(body("&quot;#{@etag}&quot;"))
  end

  test "MinIO-style numeric-entity &#34; escaping parses (the rig-caught bug)" do
    assert {:ok, @etag} = S3.copy_part_etag(body("&#34;#{@etag}&#34;"))
  end

  test "literal-quote bodies parse" do
    assert {:ok, @etag} = S3.copy_part_etag(body(~s("#{@etag}")))
  end

  test "a body with no ETag is still an error" do
    assert {:error, {:s3_part_copy_no_etag, _}} =
             S3.copy_part_etag(~s(<CopyPartResult></CopyPartResult>))
  end
end
