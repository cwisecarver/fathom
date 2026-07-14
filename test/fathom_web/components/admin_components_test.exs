defmodule FathomWeb.AdminComponentsTest do
  @moduledoc "Pure display formatters for the admin dashboard (deterministic, no DOM)."
  use ExUnit.Case, async: true

  import FathomWeb.AdminComponents

  describe "fmt_rate/1" do
    test "nil renders an em dash" do
      assert fmt_rate(nil) == "—"
    end

    test "a normal value keeps one decimal" do
      assert fmt_rate(1.5) == "1.5"
      assert fmt_rate(0.0) == "0.0"
      assert fmt_rate(9.94) == "9.9"
    end

    # Regression (expert review 2026-07-14 #24): rounding the fraction independently of the whole
    # part carried nothing — 9.96 rounded its fraction to 10 and printed "9.10", 0.98 → "0.10".
    # Formatting off the single rounded value must carry into the integer part.
    test "a fraction that rounds up to 10 carries into the whole part" do
      assert fmt_rate(9.96) == "10.0"
      assert fmt_rate(0.98) == "1.0"
      assert fmt_rate(99.95) == "100.0"
    end

    test "a value >= 1000 keeps the thousands separator" do
      assert fmt_rate(1234.5) == "1,234.5"
      # And the carry still groups: 9999.96 → 10,000.0 (not 9,999.10).
      assert fmt_rate(9999.96) == "10,000.0"
    end
  end
end
