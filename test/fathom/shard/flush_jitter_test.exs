defmodule Fathom.Shard.FlushJitterTest do
  @moduledoc """
  The durability-flush jitter knobs (expert review 2026-10-22 r8 #11).

  `SHARD_FLUSH_JITTER_RATIO` is the rig's A/B lever for flush-timer phase lock after a mass
  (re)open, so it must reach `:shard_flush_jitter_ratio` from the release env without a rebuild,
  and a malformed value must fail the boot rather than silently keep the default. Not async — it
  mutates the process environment.
  """
  use ExUnit.Case, async: false

  defp fathom_config(env_overlay) do
    prev = Map.new(env_overlay, fn {k, _} -> {k, System.get_env(k)} end)

    Enum.each(env_overlay, fn
      {k, nil} -> System.delete_env(k)
      {k, v} -> System.put_env(k, v)
    end)

    try do
      "config/runtime.exs"
      |> Config.Reader.read!(env: :test)
      |> Keyword.get(:fathom, [])
    after
      Enum.each(prev, fn
        {k, nil} -> System.delete_env(k)
        {k, v} -> System.put_env(k, v)
      end)
    end
  end

  describe "SHARD_FLUSH_JITTER_RATIO" do
    test "a number in [0, 1] is written as a float" do
      for {raw, want} <- [{"0.25", 0.25}, {"1.0", 1.0}, {"1", 1.0}, {"0", 0.0}, {" 0.5 ", 0.5}] do
        assert Keyword.fetch!(
                 fathom_config(%{"SHARD_FLUSH_JITTER_RATIO" => raw}),
                 :shard_flush_jitter_ratio
               ) == want,
               "SHARD_FLUSH_JITTER_RATIO=#{inspect(raw)}"
      end
    end

    test "unset leaves the key unwritten (the compiled/test default stands)" do
      refute Keyword.has_key?(
               fathom_config(%{"SHARD_FLUSH_JITTER_RATIO" => nil}),
               :shard_flush_jitter_ratio
             )
    end

    test "a malformed or out-of-range value is a boot error naming the variable" do
      for bad <- ["abc", "-0.1", "1.5", "0.25x", ""] do
        err =
          assert_raise RuntimeError, fn ->
            fathom_config(%{"SHARD_FLUSH_JITTER_RATIO" => bad})
          end

        assert err.message =~ "SHARD_FLUSH_JITTER_RATIO"
      end
    end
  end
end
