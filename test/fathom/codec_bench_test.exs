defmodule Fathom.CodecBenchTest do
  # Perf review 2026-10-14 #6: the compressed-flush encode path (hash + compress + body hash) on a
  # 4 MiB fixture, single-pass vs the previous shape (plaintext MD5 read, compress read, body MD5
  # read). Alternates old/new and compares medians; wall times on a loaded host are noisy, so the
  # assertion is only an order-of-magnitude one (new must not be slower than 1.1x old). The
  # deterministic part of this finding (plaintext opens 2 -> 1) is asserted in
  # s3_integrity_test.exs.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.Codec

  @moduletag :bench
  @runs 15

  defp md5_file(path) do
    path
    |> File.stream!(1024 * 1024)
    |> Enum.reduce(:crypto.hash_init(:md5), &:crypto.hash_update(&2, &1))
    |> :crypto.hash_final()
  end

  defp old_path(src, enc) do
    plain = md5_file(src)
    {:ok, tmp} = Codec.compress_to_temp(src, enc)
    body = md5_file(tmp)
    File.rm!(tmp)
    {plain, body}
  end

  defp new_path(src, enc) do
    {:ok, tmp, _size, body, plain_hex} = Codec.compress_to_temp_hashed(src, enc)
    File.rm!(tmp)
    {Base.decode16!(plain_hex, case: :lower), body}
  end

  defp median(xs), do: xs |> Enum.sort() |> Enum.at(div(length(xs), 2))

  test "single-pass encode is not slower than hash+compress+rehash, same digests" do
    src = Path.join(System.tmp_dir!(), "codec_bench_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> File.rm(src) end)

    :rand.seed(:exsss, {1, 2, 3})

    row = fn ->
      "row #{:rand.uniform(1_000_000)} payload-text...." |> String.pad_trailing(64, ".")
    end

    File.write!(src, for(_ <- 1..65_536, do: row.()) |> IO.iodata_to_binary())

    for enc <- [:zstd, :zlib] do
      assert old_path(src, enc) == new_path(src, enc)

      {olds, news} =
        Enum.reduce(1..@runs, {[], []}, fn _, {o, n} ->
          {to, _} = :timer.tc(fn -> old_path(src, enc) end)
          {tn, _} = :timer.tc(fn -> new_path(src, enc) end)
          {[to | o], [tn | n]}
        end)

      mo = median(olds)
      mn = median(news)

      IO.puts(
        "codec bench #{enc}: old median #{mo} us, new median #{mn} us (#{Float.round(100 * (mo - mn) / mo, 1)}% less)"
      )

      assert mn < mo * 1.1
    end
  end
end
