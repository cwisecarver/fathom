defmodule Fathom.DirectoryAdminTest do
  @moduledoc """
  The admin directory browse/edit context functions (expert review 2026-07-14 #22):
  `Fathom.Directory.list_page/1` and the guarded `Fathom.Directory.admin_update/2`.
  Pins the edit-safety invariant — only `status`/`retain_until` are hand-editable.
  """
  use Fathom.DataCase, async: true

  alias Fathom.Directory
  alias Fathom.Directory.Shard
  alias Fathom.Repo

  defp put_shard(attrs) do
    %Shard{}
    |> Shard.changeset(
      Enum.into(attrs, %{schema_version: 0, status: "active", last_active_at: DateTime.utc_now()})
    )
    |> Repo.insert!()
  end

  describe "admin_update/2 (guarded)" do
    test "updates the safe fields" do
      put_shard(%{shard_id: "dir_safe", status: "active"})

      until = DateTime.utc_now() |> DateTime.add(3600, :second) |> DateTime.truncate(:microsecond)

      assert {:ok, updated} =
               Directory.admin_update("dir_safe", %{status: "retired", retain_until: until})

      assert updated.status == "retired"
      assert DateTime.compare(updated.retain_until, until) == :eq
    end

    # The edit-safety boundary: migration-state fields passed in attrs are dropped by
    # admin_changeset's cast allowlist, so a hand-edit can never desync the version stamp.
    test "IGNORES schema_version and other non-safe fields in attrs" do
      put_shard(%{shard_id: "dir_guard", status: "active", schema_version: 3})

      assert {:ok, updated} =
               Directory.admin_update("dir_guard", %{
                 status: "retired",
                 schema_version: 99,
                 token_version: 42,
                 cutover_at: DateTime.utc_now()
               })

      assert updated.status == "retired"
      # Untouched — the guard dropped them.
      assert updated.schema_version == 3
      assert updated.token_version == 1
      assert updated.cutover_at == nil
    end

    test "rejects an invalid status" do
      put_shard(%{shard_id: "dir_bad", status: "active"})

      assert {:error, %Ecto.Changeset{} = cs} =
               Directory.admin_update("dir_bad", %{status: "bogus"})

      assert %{status: ["is invalid"]} = errors_on(cs)
      assert {:ok, row} = Directory.get("dir_bad")
      assert row.status == "active"
    end

    # `deleted` is a real lifecycle status but NOT hand-editable (#15): a hand-flip to
    # deleted would tombstone the row while leaving the tenant's data live and other nodes'
    # re-mint gate unset. Deletion must go through the delete orchestration, so the admin
    # edit refuses the value.
    test "rejects a hand-flip to deleted (deletion is an orchestration, not a status edit)" do
      put_shard(%{shard_id: "dir_del", status: "active"})

      assert {:error, %Ecto.Changeset{} = cs} =
               Directory.admin_update("dir_del", %{status: "deleted"})

      assert %{status: ["is invalid"]} = errors_on(cs)
      assert {:ok, row} = Directory.get("dir_del")
      assert row.status == "active"
    end

    test "can clear retain_until" do
      put_shard(%{
        shard_id: "dir_clear",
        status: "retired",
        retain_until: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      })

      assert {:ok, updated} =
               Directory.admin_update("dir_clear", %{status: "retired", retain_until: nil})

      assert updated.retain_until == nil
    end

    test "unknown shard is :not_found" do
      assert {:error, :not_found} = Directory.admin_update("dir_missing", %{status: "retired"})
    end
  end

  describe "list_page/1" do
    setup do
      put_shard(%{shard_id: "lp_alpha", status: "active"})
      put_shard(%{shard_id: "lp_beta", status: "retired"})
      put_shard(%{shard_id: "lp_gamma", status: "active"})
      :ok
    end

    test "returns all rows with a total" do
      page = Directory.list_page([])
      assert page.total == 3
      assert length(page.rows) == 3
      assert Enum.map(page.rows, & &1.shard_id) == ["lp_alpha", "lp_beta", "lp_gamma"]
    end

    test "filters by status" do
      page = Directory.list_page(status: "active")
      assert page.total == 2
      assert Enum.all?(page.rows, &(&1.status == "active"))
    end

    test "filters by shard-id substring" do
      page = Directory.list_page(q: "beta")
      assert page.total == 1
      assert [%{shard_id: "lp_beta"}] = page.rows
    end

    test "paginates with limit/offset" do
      p1 = Directory.list_page(limit: 2, offset: 0)
      assert p1.total == 3
      assert Enum.map(p1.rows, & &1.shard_id) == ["lp_alpha", "lp_beta"]

      p2 = Directory.list_page(limit: 2, offset: 2)
      assert p2.total == 3
      assert Enum.map(p2.rows, & &1.shard_id) == ["lp_gamma"]
    end

    # Review #32. AdminDirectoryLive re-runs this on every keystroke of the filter box, and
    # `total` is a second, unfiltered whole-table COUNT — at a million shards an 8-character
    # tenant name cost 8 full-table counts on top of 8 scans. `count: false` skips it.
    test "count: false skips the total but still pages correctly" do
      page = Directory.list_page(count: false)

      assert page.total == nil, "count: false must not pay for the aggregate"
      assert length(page.rows) == 3
      assert Enum.map(page.rows, & &1.shard_id) == ["lp_alpha", "lp_beta", "lp_gamma"]
    end

    # has_more? is what prev/next actually needs, and it rides the page query itself
    # (limit + 1) rather than a count.
    test "has_more? reports another page without counting" do
      assert %{has_more?: true, rows: rows} = Directory.list_page(limit: 2, count: false)
      assert Enum.map(rows, & &1.shard_id) == ["lp_alpha", "lp_beta"]

      # The extra row must never leak into the page itself.
      assert length(rows) == 2

      assert %{has_more?: false, rows: [%{shard_id: "lp_gamma"}]} =
               Directory.list_page(limit: 2, offset: 2, count: false)

      # Exactly-a-full-page is the boundary that an off-by-one gets wrong: 3 rows, limit 3.
      assert %{has_more?: false} = Directory.list_page(limit: 3, count: false)
    end

    # The JSON API publishes `total`, so the default must stay counted — turning it into
    # null would be a silent breaking change for API clients.
    test "total is still counted by default (the API contract)" do
      assert %{total: 3} = Directory.list_page([])
      assert %{total: 2} = Directory.list_page(status: "active")
    end
  end
end
