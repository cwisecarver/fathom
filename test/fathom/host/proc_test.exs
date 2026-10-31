defmodule Fathom.Host.ProcTest do
  # Expert review 2026-10-29 #1 / #17: the pure parsers behind the dirty-writeback boot preflight and
  # the host gauges. They take file CONTENTS, so none of this needs /proc (or Linux).
  use ExUnit.Case, async: true

  alias Fathom.Host.Proc

  @gib 1024 * 1024 * 1024

  # A 94 GB host, as on the chaos rig: MemAvailable ~ 92 GiB.
  @meminfo """
  MemTotal:       98566144 kB
  MemFree:        60000000 kB
  MemAvailable:   96468992 kB
  Active(anon):      12345 kB
  Dirty:              1024 kB
  Writeback:            64 kB
  HugePages_Total:       0
  """

  describe "effective_background_bytes/1" do
    test "a default large-RAM host (bytes 0, ratio 10) resolves to the ratio of MemAvailable" do
      meminfo = Proc.parse_meminfo(@meminfo)
      bytes = Proc.effective_background_bytes(%{bytes: 0, ratio: 10, meminfo: meminfo})

      assert bytes == div(96_468_992 * 1024 * 10, 100)
      assert bytes > Proc.warn_above_bytes(), "the rig's 9.4 GB default must be over the line"
    end

    test "a 1 GB cap is not over the line" do
      meminfo = Proc.parse_meminfo(@meminfo)
      bytes = Proc.effective_background_bytes(%{bytes: @gib, ratio: 0, meminfo: meminfo})

      assert bytes == @gib
      refute bytes > Proc.warn_above_bytes()
    end

    test "dirty_background_bytes wins over the ratio whenever it is non-zero" do
      meminfo = Proc.parse_meminfo(@meminfo)

      # The kernel zeroes one of the pair when the other is written, so ratio=10 beside a non-zero
      # bytes is not a state it produces; the bytes value must win regardless.
      assert Proc.effective_background_bytes(%{bytes: @gib, ratio: 10, meminfo: meminfo}) == @gib
    end

    test "the ratio uses MemTotal on a kernel without MemAvailable" do
      meminfo = %{"MemTotal" => 10 * @gib}
      assert Proc.effective_background_bytes(%{bytes: 0, ratio: 20, meminfo: meminfo}) == 2 * @gib
    end

    test "a small host on the default ratio is far under the line" do
      meminfo = %{"MemAvailable" => 8 * @gib}
      bytes = Proc.effective_background_bytes(%{bytes: 0, ratio: 10, meminfo: meminfo})

      assert bytes < @gib
    end

    test "zero bytes and zero ratio is a zero threshold, not an unknown one" do
      assert Proc.effective_background_bytes(%{bytes: 0, ratio: 0, meminfo: %{"MemTotal" => 1}}) ==
               0
    end

    test "unreadable inputs are nil, never a guess" do
      assert Proc.effective_background_bytes(%{}) == nil
      assert Proc.effective_background_bytes(%{bytes: nil, ratio: nil, meminfo: %{}}) == nil
      # A ratio with no memory figure to apply it to.
      assert Proc.effective_background_bytes(%{bytes: 0, ratio: 10, meminfo: %{}}) == nil
    end

    test "the warn line is 2 GiB and exclusive" do
      assert Proc.warn_above_bytes() == 2 * @gib
    end
  end

  describe "parse_int/1" do
    test "reads a /proc/sys integer file" do
      assert Proc.parse_int("1073741824\n") == 1_073_741_824
      assert Proc.parse_int("0\n") == 0
    end

    test "anything else is nil" do
      assert Proc.parse_int("") == nil
      assert Proc.parse_int("10 20\n") == nil
      assert Proc.parse_int("-1\n") == nil
      assert Proc.parse_int("abc") == nil
    end
  end

  describe "parse_meminfo/1" do
    test "kB rows become bytes and unitless rows keep their count" do
      m = Proc.parse_meminfo(@meminfo)

      assert m["MemAvailable"] == 96_468_992 * 1024
      assert m["Dirty"] == 1024 * 1024
      assert m["Writeback"] == 64 * 1024
      assert m["Active(anon)"] == 12_345 * 1024
      assert m["HugePages_Total"] == 0
    end

    test "garbage yields an empty map" do
      assert Proc.parse_meminfo("not meminfo\n") == %{}
      assert Proc.parse_meminfo("") == %{}
    end
  end

  describe "parse_pressure/1" do
    test "reads some and full avg10" do
      body = """
      some avg10=1.25 avg60=0.50 avg300=0.10 total=123456
      full avg10=0.75 avg60=0.20 avg300=0.05 total=65432
      """

      assert Proc.parse_pressure(body) == %{some_avg10: 1.25, full_avg10: 0.75}
    end

    test "a file with only some, or none, reports what it has" do
      assert Proc.parse_pressure("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n") ==
               %{some_avg10: 0.0}

      assert Proc.parse_pressure("") == %{}
    end
  end
end
