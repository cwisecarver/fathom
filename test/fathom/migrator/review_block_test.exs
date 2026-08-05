defmodule Fathom.Migrator.ReviewBlockTest do
  @moduledoc """
  Expert review 2026-08-01 #26, part 1: make the block LEGIBLE.

  `GET /api/migrations/status` reported `pending_review: [7]` and nothing else. An operator saw
  `converged: false` with every later migration stacked behind version 7 and no way to learn what
  tripped it or what their options were — so the practical outcome was "the fleet stops and nobody
  knows why".

  Part 2 is `attach_transform/2`, the third path out of that block. Its guards are here too,
  because each of them is a way the block could be cleared UNSAFELY, which is worse than the block.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Migrator
  alias Fathom.Migrator.Release

  defmodule OkTransform do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(_conn, _shard_id), do: :ok
  end

  setup do
    prev = Application.get_env(:fathom, :migration_transforms)
    Application.put_env(:fathom, :migration_transforms, [OkTransform])

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :migration_transforms, prev),
        else: Application.delete_env(:fathom, :migration_transforms)
    end)

    :ok
  end

  defp insert_release(attrs) do
    %Release{}
    |> Ecto.Changeset.change(
      Map.merge(
        %{version: System.unique_integer([:positive]), name: "test", statements: []},
        attrs
      )
    )
    |> Repo.insert!()
  end

  describe "review_block/1 explains the hold" do
    test "a data migration reports the flagged statements and both options" do
      r =
        insert_release(%{
          requires_review: true,
          review_reason: "data_migration",
          review_detail: %{"statements" => ["UPDATE orders SET total = 42"]},
          statements: ["ALTER TABLE orders ADD COLUMN total INT", "UPDATE orders SET total = 42"]
        })

      block = Migrator.review_block(r)

      assert block.version == r.version
      assert block.reason == "data_migration"
      assert block.detail["statements"] == ["UPDATE orders SET total = 42"]

      actions = Enum.map(block.options, & &1.action)
      assert "attach_transform" in actions
      assert "approve_review" in actions

      # The dangerous option must say what it actually does. An operator reading only this should
      # understand that approving replays the TEMPLATE's rows onto every tenant.
      approve = Enum.find(block.options, &(&1.action == "approve_review"))
      assert approve.effect =~ "TEMPLATE"
      assert approve.effect =~ "every tenant"
    end

    test "a migration gap offers reconcile, NOT approve-or-transform" do
      # A gap means the template ran a migration capture never saw (atomic = False). The fleet is
      # missing DDL, so both of the other options would paper over a real divergence.
      r = insert_release(%{requires_review: true, review_reason: "migration_gap"})

      block = Migrator.review_block(r)
      actions = Enum.map(block.options, & &1.action)

      assert actions == ["reconcile_template"]
      refute "approve_review" in actions
      refute "attach_transform" in actions
    end

    test "both reasons offer the gap resolution first" do
      r = insert_release(%{requires_review: true, review_reason: "data_migration_and_gap"})
      block = Migrator.review_block(r)

      assert hd(block.options).action == "reconcile_template"
      assert length(block.options) == 3
    end

    test "a release captured BEFORE #26 still gets a legible block" do
      # No stored reason (the column did not exist). Re-derived from the statements, so an existing
      # frozen fleet gets the explanation on upgrade rather than only new captures.
      r =
        insert_release(%{
          requires_review: true,
          review_reason: nil,
          statements: ["ALTER TABLE t ADD COLUMN x INT", "DELETE FROM t WHERE x IS NULL"]
        })

      block = Migrator.review_block(r)

      assert block.reason == "data_migration"
      assert block.detail["statements"] == ["DELETE FROM t WHERE x IS NULL"]
      assert "attach_transform" in Enum.map(block.options, & &1.action)
    end

    test "status/0 adds review_blocks WITHOUT changing pending_review's shape" do
      # `review_blocks` is additive on purpose. The first draft replaced `pending_review` with the
      # block objects, and `migration_controller_test` failed on `pending_review == [2]` — which is
      # a published control-plane endpoint telling you a field changing type is a break for its
      # consumers. Both fields now ship: the old one for compatibility, the new one for legibility.
      r =
        insert_release(%{
          requires_review: true,
          review_reason: "data_migration",
          statements: ["UPDATE t SET x = 1"]
        })

      status = Migrator.status()

      assert r.version in status.pending_review

      assert Enum.all?(status.pending_review, &is_integer/1),
             "pending_review stays version numbers"

      block = Enum.find(status.review_blocks, &(&1.version == r.version))
      assert block.reason == "data_migration"
      assert block.options != []
    end
  end

  describe "attach_transform/2 guards" do
    test "attaches a registered module and clears the block" do
      r = insert_release(%{requires_review: true, review_reason: "data_migration"})

      assert :ok = Migrator.attach_transform(r.version, OkTransform)

      reloaded = Repo.get_by(Release, version: r.version)
      assert reloaded.transform == to_string(OkTransform)
      refute reloaded.requires_review
      assert reloaded.review_reason == nil
    end

    test "refuses a module that is not on the allowlist" do
      r = insert_release(%{requires_review: true, review_reason: "data_migration"})

      assert {:error, :not_allowed} = Migrator.attach_transform(r.version, Enum)
      assert Repo.get_by(Release, version: r.version).requires_review, "the block must remain"
    end

    test "refuses a registered module that does not export run/2" do
      Application.put_env(:fathom, :migration_transforms, [Enum])
      r = insert_release(%{requires_review: true, review_reason: "data_migration"})

      assert {:error, :invalid_transform} = Migrator.attach_transform(r.version, Enum)
    end

    test "refuses while template-literal DML is still on the release" do
      # Otherwise the version would run BOTH: the template's literal rows AND the transform.
      r =
        insert_release(%{
          requires_review: true,
          review_reason: "data_migration",
          statements: ["ALTER TABLE t ADD COLUMN x INT", "UPDATE t SET x = 42"]
        })

      assert {:error, {:data_statements_present, ["UPDATE t SET x = 42"]}} =
               Migrator.attach_transform(r.version, OkTransform)

      assert Repo.get_by(Release, version: r.version).requires_review
    end

    test "refuses a version held for a migration GAP" do
      # A transform cannot conjure the DDL the fleet missed; clearing the flag here would hide a
      # real template/fleet divergence behind a backfill.
      r = insert_release(%{requires_review: true, review_reason: "migration_gap"})

      assert {:error, :gap_requires_reconcile} = Migrator.attach_transform(r.version, OkTransform)
      assert Repo.get_by(Release, version: r.version).requires_review
    end

    test "refuses an unknown version" do
      assert {:error, :unknown_version} = Migrator.attach_transform(999_999, OkTransform)
    end

    test "an attached version becomes appliable and carries its transform in the chain step" do
      r =
        insert_release(%{
          requires_review: true,
          review_reason: "data_migration",
          statements: ["ALTER TABLE t ADD COLUMN x INT"]
        })

      # Held: the rollout refuses to build a chain through it.
      assert Migrator.statement_step(r.version) == nil

      assert :ok = Migrator.attach_transform(r.version, OkTransform)

      assert {pairs, transform} = Migrator.statement_step(r.version)
      assert transform == to_string(OkTransform)
      assert [{"ALTER TABLE t ADD COLUMN x INT", []}] = pairs
    end
  end
end
