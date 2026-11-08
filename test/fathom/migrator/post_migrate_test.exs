defmodule Fathom.Migrator.PostMigrateTest do
  @moduledoc """
  Expert review 2026-11-07 #53 — the pure half: recognizing Django's `post_migrate` INSERTs,
  rewriting them to natural-key idempotent SQL, and what `Capture.data_migration_statements/1` makes
  of the result.

  The statements under test are the VERBATIM ones from `test/support/fixtures/post_migrate_capture.json`
  (a real `manage.py migrate` adding `widgets.0002_gadget`), not hand-written look-alikes.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Fathom.Migrator.{Capture, PostMigrate}

  @fixture "test/support/fixtures/post_migrate_capture.json"

  setup_all do
    events = @fixture |> File.read!() |> Jason.decode!() |> Map.fetch!("events")

    ct =
      Enum.find(
        events,
        &String.starts_with?(&1["sql"] || "", ~s|INSERT INTO "django_content_type"|)
      )

    perm =
      Enum.find(events, &String.starts_with?(&1["sql"] || "", ~s|INSERT INTO "auth_permission"|))

    %{ct: {ct["sql"], ct["args"]}, perm: {perm["sql"], perm["args"]}}
  end

  # What the template's `django_content_type` table says for the ids the fixture binds.
  defp lookup(%{} = keys), do: fn ids -> Map.take(keys, ids) end
  defp no_lookup, do: fn _ids -> flunk("lookup must not be called for this statement") end

  describe "shape/2" do
    test "recognizes the verbatim INSERTs", %{ct: {ct_sql, ct_args}, perm: {perm_sql, perm_args}} do
      # Preconditions: the fixture really is the multi-row, RETURNING form Django 5.0 emits.
      assert ct_sql =~ "RETURNING"
      assert length(perm_args) == 12 and perm_sql =~ "(?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)"

      assert PostMigrate.shape(ct_sql, ct_args) == :content_type
      assert PostMigrate.shape(perm_sql, perm_args) == :permission
    end

    test "tolerates a missing RETURNING clause and any row count", %{ct: {ct_sql, _}} do
      bare = String.replace(ct_sql, ~r/ RETURNING.*/, "")
      assert PostMigrate.shape(bare, ["a", "m"]) == :content_type

      three =
        ~s|INSERT INTO "django_content_type" ("app_label", "model") VALUES | <>
          Enum.join(List.duplicate("(?, ?)", 3), ", ")

      assert PostMigrate.shape(three, ~w(a1 m1 a2 m2 a3 m3)) == :content_type
    end

    test "anything but the exact shape is not post_migrate", %{
      ct: {ct_sql, ct_args},
      perm: {perm_sql, perm_args}
    } do
      # placeholder groups disagree with the bound args
      assert PostMigrate.shape(ct_sql, ["only-one"]) == nil
      assert PostMigrate.shape(ct_sql, ["a", "m", "extra"]) == nil
      assert PostMigrate.shape(perm_sql, Enum.take(perm_args, 11)) == nil
      assert PostMigrate.shape(ct_sql, []) == nil

      # a different column list / clause / table / casing / verb
      assert PostMigrate.shape(String.replace(ct_sql, ~s|"model"|, ~s|"model", "extra"|), ct_args) ==
               nil

      assert PostMigrate.shape(ct_sql <> " ON CONFLICT DO NOTHING", ct_args) == nil
      assert PostMigrate.shape(String.downcase(ct_sql), ct_args) == nil

      assert PostMigrate.shape(
               String.replace(ct_sql, "INSERT INTO", "INSERT OR IGNORE INTO"),
               ct_args
             ) == nil

      assert PostMigrate.shape(
               String.replace(perm_sql, "auth_permission", "auth_group"),
               perm_args
             ) == nil

      assert PostMigrate.shape("UPDATE auth_permission SET codename = ?", ["x"]) == nil
      assert PostMigrate.shape(nil, []) == nil
    end
  end

  describe "capture_pairs/3" do
    test "a content-type INSERT becomes one idempotent natural-key statement per row", %{
      ct: {sql, args}
    } do
      assert [{rewritten, ["widgets", "gadget", "widgets", "gadget"]}] =
               PostMigrate.capture_pairs(sql, args, no_lookup())

      assert PostMigrate.rewritten?(rewritten)
      refute rewritten =~ "VALUES"
      assert rewritten =~ "NOT EXISTS"
    end

    test "a permission INSERT is resolved id -> (app_label, model), one statement per row", %{
      perm: {sql, args}
    } do
      # The fixture binds the TEMPLATE's literal id 6; the rewrite must carry the natural key instead.
      assert Enum.at(args, 1) == 6

      pairs = PostMigrate.capture_pairs(sql, args, lookup(%{6 => {"widgets", "gadget"}}))

      assert [
               {s, ["Can add gadget", "add_gadget", "widgets", "gadget", "add_gadget"]},
               {s, ["Can change gadget", "change_gadget", "widgets", "gadget", "change_gadget"]},
               {s, ["Can delete gadget", "delete_gadget", "widgets", "gadget", "delete_gadget"]},
               {s, ["Can view gadget", "view_gadget", "widgets", "gadget", "view_gadget"]}
             ] = pairs

      assert PostMigrate.rewritten?(s)

      # No template-literal id anywhere in what gets recorded.
      refute Enum.any?(pairs, fn {_, a} -> 6 in a end)
    end

    test "is deterministic", %{perm: {sql, args}} do
      keys = lookup(%{6 => {"widgets", "gadget"}})

      assert PostMigrate.capture_pairs(sql, args, keys) ==
               PostMigrate.capture_pairs(sql, args, keys)
    end

    test "an id the template cannot resolve leaves the statement RAW (and says so)", %{
      perm: {sql, args}
    } do
      log =
        capture_log(fn ->
          assert [{^sql, ^args}] = PostMigrate.capture_pairs(sql, args, lookup(%{}))
        end)

      assert log =~ "cannot resolve auth_permission content_type_id 6"
    end

    test "a non-text or non-integer binding is left raw", %{
      ct: {ct_sql, _},
      perm: {perm_sql, perm_args}
    } do
      assert [{^ct_sql, [nil, "gadget"]}] =
               PostMigrate.capture_pairs(ct_sql, [nil, "gadget"], no_lookup())

      bad = List.replace_at(perm_args, 1, "6")
      assert [{^perm_sql, ^bad}] = PostMigrate.capture_pairs(perm_sql, bad, no_lookup())
    end

    test "every other statement is returned unchanged and never touches the lookup" do
      for sql <- [
            "CREATE TABLE t (id INTEGER)",
            ~s|INSERT INTO "django_migrations" ("app", "name", "applied") VALUES (?, ?, ?)|,
            ~s|INSERT INTO "widgets_widget" ("name") VALUES (?)|
          ] do
        assert [{^sql, ["x"]}] = PostMigrate.capture_pairs(sql, ["x"], no_lookup())
      end
    end
  end

  describe "rewritten?/1" do
    test "is exactly the two emitted statements", %{
      ct: {ct_sql, ct_args},
      perm: {perm_sql, perm_args}
    } do
      [{ct, _}] = PostMigrate.capture_pairs(ct_sql, ct_args, no_lookup())

      [{perm, _} | _] =
        PostMigrate.capture_pairs(perm_sql, perm_args, lookup(%{6 => {"widgets", "gadget"}}))

      assert PostMigrate.rewritten?(ct)
      assert PostMigrate.rewritten?(perm)

      refute PostMigrate.rewritten?(ct <> " ")
      refute PostMigrate.rewritten?(String.replace(perm, "NOT EXISTS", "EXISTS"))
      refute PostMigrate.rewritten?(ct_sql)
      refute PostMigrate.rewritten?(perm_sql)
      refute PostMigrate.rewritten?(nil)
    end
  end

  # The classifier that otherwise holds unknown writes for operator review.
  describe "Capture.data_migration_statements/1" do
    test "does not flag the rewritten statements, still flags the RAW verbatim ones", %{
      ct: {ct_sql, ct_args},
      perm: {perm_sql, perm_args}
    } do
      [{ct, _}] = PostMigrate.capture_pairs(ct_sql, ct_args, no_lookup())

      [{perm, _} | _] =
        PostMigrate.capture_pairs(perm_sql, perm_args, lookup(%{6 => {"widgets", "gadget"}}))

      assert Capture.data_migration_statements([ct, perm]) == []

      # The raw form carries the template's literal id: it IS the poisoning shape.
      assert Capture.data_migration_statements([ct_sql, perm_sql]) == [ct_sql, perm_sql]
    end

    test "nothing broader is taught: other writes to the same tables stay flagged" do
      for sql <- [
            ~s|UPDATE "auth_permission" SET "codename" = ? WHERE "id" = ?|,
            ~s|DELETE FROM "django_content_type" WHERE "app_label" = ?|,
            ~s|INSERT INTO "auth_group" ("name") VALUES (?)|,
            ~s|INSERT INTO "auth_permission" ("name", "content_type_id", "codename") VALUES (?, 99, ?)|
          ] do
        assert Capture.data_migration_statements([sql]) == [sql]
      end
    end
  end
end
