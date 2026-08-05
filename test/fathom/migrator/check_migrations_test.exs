defmodule Fathom.Migrator.CheckMigrationsTest do
  @moduledoc """
  Expert review 2026-08-01 #26, third part: `atomic = False` migrations.

  These run in autocommit — outside any tracked `BEGIN…COMMIT` — so `Capture` **cannot see them by
  construction**. The gap detector catches the consequence at the NEXT capture, by which point the
  template has already moved ahead and that next version is flagged too, so the backstop fires late
  and blames the wrong migration. This check runs before anything touches the template.

  The fixtures are real Django migration shapes, not invented approximations: a lint that matched
  something Django never emits would prove nothing.
  """
  use ExUnit.Case, async: true

  alias Mix.Tasks.Fathom.CheckMigrations

  @tmp Path.join(System.tmp_dir!(), "fathom_checkmig")

  setup do
    dir = Path.join(@tmp, "t#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  defp write(dir, name, body) do
    path = Path.join(dir, name)
    File.write!(path, body)
    path
  end

  describe "atomic = False" do
    test "is an ERROR — capture cannot see it at all", %{dir: dir} do
      path =
        write(dir, "0002_add_index.py", """
        from django.db import migrations, models


        class Migration(migrations.Migration):
            atomic = False

            dependencies = [("app", "0001_initial")]

            operations = [
                migrations.AddIndex(
                    model_name="order",
                    index=models.Index(fields=["created"], name="order_created_idx"),
                ),
            ]
        """)

      assert [finding] = CheckMigrations.scan(path)
      assert finding.severity == "error"
      assert finding.rule == "atomic_false"
      assert finding.message =~ "autocommit"
    end

    test "an ordinary atomic migration is clean", %{dir: dir} do
      path =
        write(dir, "0003_add_field.py", """
        from django.db import migrations, models


        class Migration(migrations.Migration):
            dependencies = [("app", "0002_add_index")]

            operations = [
                migrations.AddField(
                    model_name="order",
                    name="total",
                    field=models.IntegerField(default=0),
                ),
            ]
        """)

      assert CheckMigrations.scan(path) == []
    end

    test "does not fire on the string appearing elsewhere", %{dir: dir} do
      # `atomic` in a comment, a docstring, or a different assignment must not trip it — a lint
      # that cries wolf gets disabled, and then it catches nothing.
      path =
        write(dir, "0004_comment.py", """
        from django.db import migrations


        class Migration(migrations.Migration):
            # We deliberately do NOT set atomic = False here; see docs/django-migrations.md
            dependencies = []
            operations = []
            some_other_flag = False
        """)

      assert CheckMigrations.scan(path) == []
    end

    test "fires regardless of indentation", %{dir: dir} do
      path = write(dir, "0005_indent.py", "class Migration:\n        atomic = False\n")
      assert [%{rule: "atomic_false"}] = CheckMigrations.scan(path)
    end
  end

  describe "data migrations" do
    test "RunPython is a WARNING with the supported path named", %{dir: dir} do
      # A warning, not an error: RunPython is legitimate and now has a route through
      # attach_transform/2. The point is to say so BEFORE the fleet freezes.
      path =
        write(dir, "0006_backfill.py", """
        from django.db import migrations


        def backfill(apps, schema_editor):
            Order = apps.get_model("app", "Order")
            for o in Order.objects.all():
                o.total = o.qty * o.price
                o.save()


        class Migration(migrations.Migration):
            dependencies = [("app", "0005_add_total")]
            operations = [migrations.RunPython(backfill, migrations.RunPython.noop)]
        """)

      assert [finding] = CheckMigrations.scan(path)
      assert finding.severity == "warning"
      assert finding.rule == "run_python"
      assert finding.message =~ "attach_transform"
    end

    test "RunSQL is a warning", %{dir: dir} do
      path =
        write(dir, "0007_sql.py", """
        from django.db import migrations


        class Migration(migrations.Migration):
            operations = [migrations.RunSQL("UPDATE app_order SET total = 0")]
        """)

      assert [%{rule: "run_sql", severity: "warning"}] = CheckMigrations.scan(path)
    end

    test "a migration that is BOTH non-atomic and a data migration reports both", %{dir: dir} do
      path =
        write(dir, "0008_both.py", """
        from django.db import migrations


        class Migration(migrations.Migration):
            atomic = False
            operations = [migrations.RunPython(lambda a, s: None)]
        """)

      findings = CheckMigrations.scan(path)
      rules = Enum.map(findings, & &1.rule) |> Enum.sort()
      assert rules == ["atomic_false", "run_python"]
    end
  end

  describe "discovery" do
    test "finds migrations under app/migrations/ and skips __init__.py", %{dir: dir} do
      File.mkdir_p!(Path.join(dir, "orders/migrations"))
      write(dir, "orders/migrations/__init__.py", "")
      write(dir, "orders/migrations/0001_initial.py", "class Migration: pass\n")
      write(dir, "orders/migrations/0002_x.py", "class Migration:\n    atomic = False\n")

      files = CheckMigrations.migration_files(dir)

      assert length(files) == 2
      refute Enum.any?(files, &(Path.basename(&1) == "__init__.py"))
      assert Enum.flat_map(files, &CheckMigrations.scan/1) |> length() == 1
    end

    test "accepts a single file path", %{dir: dir} do
      path = write(dir, "0001_initial.py", "class Migration: pass\n")
      assert CheckMigrations.migration_files(path) == [path]
    end
  end
end
