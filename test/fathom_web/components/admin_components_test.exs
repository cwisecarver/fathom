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

  describe "fmt_rel/2" do
    @now ~U[2026-10-16 12:00:00Z]

    test "past values pick the largest whole unit" do
      assert fmt_rel(DateTime.add(@now, -4), @now) == "4s ago"
      assert fmt_rel(DateTime.add(@now, -90), @now) == "1m ago"
      assert fmt_rel(DateTime.add(@now, -3 * 3600), @now) == "3h ago"
      assert fmt_rel(DateTime.add(@now, -15 * 86_400), @now) == "15d ago"
    end

    test "future values read as 'in', nil as an em dash" do
      assert fmt_rel(DateTime.add(@now, 2 * 86_400), @now) == "in 2d"
      assert fmt_rel(nil, @now) == "—"
    end
  end

  describe "rel_time/1 and load_empty_reason/1" do
    import Phoenix.LiveViewTest

    # Symptom (browser review 2026-10-16): raw ISO timestamps wrapped to two lines. The cell is now
    # relative + nowrap, with the full UTC timestamp kept in `title`.
    test "rel_time carries the ISO timestamp in a title" do
      at = DateTime.add(DateTime.utc_now(), -3 * 3600)
      html = render_component(&rel_time/1, id: "t", at: at)
      assert html =~ ~s(title="#{DateTime.to_iso8601(at)}")
      assert html =~ "whitespace-nowrap"
      assert html =~ "3h ago"
    end

    # Symptom: the empty state said "set SHARD_LOAD=true" even when it was already true; the
    # missing piece was the reporter. Every branch names the thing that is actually missing.
    test "load_empty_reason names what is actually missing" do
      off = render_component(&load_empty_reason/1, load: false, reporter: false)
      assert off =~ "load-reason-load-off"
      assert off =~ "SHARD_LOAD=true"

      rep = render_component(&load_empty_reason/1, load: true, reporter: false)
      assert rep =~ "load-reason-reporter-off"
      assert rep =~ "LOAD_REPORTER=true"
      refute rep =~ "SHARD_LOAD=true"

      none = render_component(&load_empty_reason/1, load: true, reporter: true)
      assert none =~ "load-reason-no-traffic"

      # Node scope does not need the reporter.
      node =
        render_component(&load_empty_reason/1, load: true, reporter: false, scope: :node)

      assert node =~ "load-reason-no-traffic"
    end
  end
end
