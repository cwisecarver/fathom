defmodule Fathom.Migrator.LedgerTest do
  @moduledoc """
  `Fathom.Migrator.Ledger.classify/4` — Django's ledger by NAME against the version label — and the
  name extraction that feeds it. The classification is pure, so every verdict is pinned here
  without a shard; `shard_migration_test.exs` drives it through real migrations.
  """
  # NOT async. Several tests here insert release rows at versions 1 and 2, and `shard_migrations`
  # has a unique index on `version`. `migrator_test.exs` (async) inserts the same versions, and two
  # sandboxed transactions inserting the same unique keys in different orders DEADLOCK in Postgres
  # (40P01) — reproducible with `mix test --seed 427150` (2026-10-09). The sandbox isolates what each
  # test sees, not the index locks taken while inserting.
  use Fathom.DataCase, async: false

  import Ecto.Query

  alias Fathom.Migrator.Ledger
  alias Fathom.Migrator.Release

  defp n(name), do: {"app", name}
  defp names(list), do: MapSet.new(list, &n/1)
  defp fact(list, count \\ nil), do: %{names: names(list), count: count}

  # v1 adds 0001, v2 adds 0002, v3 adds 0003.
  @facts %{
    1 => %{names: MapSet.new([{"app", "0001"}]), count: nil},
    2 => %{names: MapSet.new([{"app", "0002"}]), count: nil},
    3 => %{names: MapSet.new([{"app", "0003"}]), count: nil}
  }

  describe "classify/4" do
    test "a ledger that matches its label is :ok" do
      assert :ok = Ledger.classify(2, names(~w(0001 0002)), 2, @facts)
    end

    test "names belonging to no release (the template's baseline) are not evidence" do
      assert :ok = Ledger.classify(2, names(~w(baseline 0001 0002)), 3, @facts)
    end

    test "a label AHEAD of a clean ledger prefix is {:behind, k} — the real version" do
      # The file says v3, but Django applied only through v1.
      assert {:behind, 1} = Ledger.classify(3, names(~w(0001)), 1, @facts)
    end

    test "a name from a release ABOVE the label is a mismatch, not a repair" do
      assert {:mismatch, %{unexpected: [{3, {"app", "0003"}}]}} =
               Ledger.classify(2, names(~w(0001 0002 0003)), 3, @facts)
    end

    test "a hole that is not a clean prefix is a mismatch" do
      # 0001 missing, 0002 present: no version k explains it.
      assert {:mismatch, %{candidates: [], missing: [{1, {"app", "0001"}}]}} =
               Ledger.classify(2, names(~w(0002)), 1, @facts)
    end

    test "a partially applied release is a mismatch" do
      facts = %{1 => fact(~w(0001a 0001b))}
      assert {:mismatch, _} = Ledger.classify(1, names(~w(0001a)), 1, facts)
    end

    test "an unnamed release between candidates makes the real version ambiguous" do
      # v2 carries no bookkeeping rows, so a ledger through v1 fits k=1 AND k=2. The label (3) is
      # wrong, but replaying from the wrong one of those would skip or re-run v2's DDL.
      facts = Map.put(@facts, 2, fact([]))
      assert {:mismatch, %{candidates: [1, 2]}} = Ledger.classify(3, names(~w(0001)), 1, facts)
    end

    test "a known template count that disagrees is a mismatch even when every name fits" do
      # An extra migration run against the shard directly: its name belongs to no release.
      facts = Map.put(@facts, 2, fact(~w(0002), 2))

      assert {:mismatch, %{expected_count: 2, count: 3}} =
               Ledger.classify(2, names(~w(0001 0002 rogue)), 3, facts)
    end

    test "no named releases at all is no evidence — :ok, as before the check existed" do
      assert :ok = Ledger.classify(5, names(~w(whatever)), 1, %{1 => fact([]), 5 => fact([])})
    end

    test "a missing ledger at a released version reads as behind at v0, not as a mismatch" do
      # Deliberate. An empty ledger IS a clean prefix (nothing applied), so the replay starts from
      # v0. That is right if the schema really is empty, and SAFE if it is not: v1's DDL then fails
      # inside its own transaction on a temp copy, nothing is published, and the migration is
      # refused. The restore drill's count check reported this as `:ledger_mismatch`; the migrator
      # can do better than refuse because it can try the replay without risk.
      assert {:behind, 0} = Ledger.classify(1, MapSet.new(), 0, @facts)
    end
  end

  describe "check_file/1 reads each release's names once per node (expert review 2026-10-01 perf #12)" do
    # A release's statements are written once at capture and never updated, so its names are a
    # pure function of the release. check_file/1 used to re-derive every release's names (one
    # in-memory SQLite open + replay per release) on EVERY call, twice per migrated shard, so a
    # fleet rollout paid O(releases) SQLite opens per shard. These pin that a release's names are
    # derived once and reused.
    defp bookkeeping_release!(version, name) do
      %Release{}
      |> Release.changeset(%{
        version: version,
        name: "v#{version}",
        statements: [
          ~s|INSERT INTO "django_migrations" ("app", "name", "applied") VALUES (?, ?, ?)|
        ],
        statement_args: [%{"args" => Enum.map(["app", name, "2026-01-01"], &Filo.Value.encode/1)}]
      })
      |> Fathom.Repo.insert!()
    end

    defp ledger_file!(version, names) do
      path = Path.join(System.tmp_dir!(), "ledgermemo_#{System.unique_integer([:positive])}.db")
      on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
      {:ok, conn} = Fathom.Shard.Connection.open(path)

      :ok =
        Fathom.Shard.Connection.exec(
          conn,
          "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY, app TEXT, name TEXT, applied TEXT)"
        )

      for n <- names do
        {:ok, _} =
          Fathom.Shard.Connection.query(
            conn,
            "INSERT INTO django_migrations (app, name, applied) VALUES ('app', ?, 'x')",
            [n]
          )
      end

      :ok = Fathom.Shard.Connection.exec(conn, "PRAGMA user_version = #{version}")
      :ok = Fathom.Shard.Connection.close(conn)
      path
    end

    test "a release's names are derived once, not on every check" do
      release = bookkeeping_release!(1, "0001_memo")
      path = ledger_file!(1, ["0001_memo"])

      assert {1, :ok} = Ledger.check_file(path)

      # Rewrite the stored payload behind the cache's back. Production never does this (statements
      # are immutable after capture); here it is the observable: a check that re-derived the names
      # would now see "0001_other", call the shard's ledger a mismatch, and fail.
      bad = [~s|INSERT INTO "django_migrations" ("app", "name", "applied") VALUES (?, ?, ?)|]

      Fathom.Repo.update_all(
        from(r in Release, where: r.id == ^release.id),
        set: [
          statements: bad,
          statement_args: [
            %{"args" => Enum.map(["app", "0001_other", "2026-01-01"], &Filo.Value.encode/1)}
          ]
        ]
      )

      assert {1, :ok} = Ledger.check_file(path),
             "check_file/1 re-derived a release's names instead of reusing them"
    end

    test "a yank is still seen after the names are cached" do
      _ = bookkeeping_release!(1, "0001_y")
      r2 = bookkeeping_release!(2, "0002_y")

      # Label 1, but it also holds release 2's name: a migration the label does not admit to.
      path = ledger_file!(1, ["0001_y", "0002_y"])
      assert {1, {:mismatch, _}} = Ledger.check_file(path)

      # Yanking is MUTABLE, unlike the statements: the chain may skip a yanked release, so its
      # names stop being evidence. The cache must not freeze the yank out.
      Fathom.Repo.update_all(from(r in Release, where: r.id == ^r2.id), set: [yanked: true])

      assert {1, :ok} = Ledger.check_file(path),
             "the yank was not seen: a cached release kept counting after it was yanked"
    end
  end

  describe "release_names/1 — SQLite parses Django's own INSERTs" do
    test "reads names from parameterized bookkeeping rows and their bound values" do
      release = %Release{
        statements: [
          "CREATE TABLE app_x (id INTEGER PRIMARY KEY)",
          ~s|INSERT INTO "django_migrations" ("app", "name", "applied") VALUES (?, ?, ?)|
        ],
        statement_args: [
          %{"args" => []},
          %{"args" => Enum.map(["app", "0007_x", "2026-01-01"], &Filo.Value.encode/1)}
        ]
      }

      assert Ledger.release_names(release) == MapSet.new([{"app", "0007_x"}])
    end

    test "reads literal rows, including one that omits `applied`" do
      release = %Release{
        statements: [
          "INSERT INTO django_migrations (app, name, applied) VALUES ('a', '0001', 'now')",
          "INSERT INTO django_migrations (app, name) VALUES ('b', '0002')"
        ],
        statement_args: nil
      }

      assert Ledger.release_names(release) == MapSet.new([{"a", "0001"}, {"b", "0002"}])
    end

    test "a release with no bookkeeping rows has no names" do
      release = %Release{statements: ["CREATE TABLE t (a)"], statement_args: nil}
      assert Ledger.release_names(release) == MapSet.new()
    end
  end
end
