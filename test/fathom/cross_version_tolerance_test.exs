defmodule Fathom.CrossVersionToleranceTest do
  @moduledoc """
  The migration gate's third item, which had no test (expert review 2026-08-01 #48).

  `AGENTS.md` § Gates requires of every schema migration:

  > (c) a cross-version-tolerance check — "during a rollout the fleet is mixed `vN-1`/`vN`; assert
  > the app reads both"

  `grep -rl "cross.version\\|mixed.window" test/` returned only `snapshots_schema_guard_test.exs`,
  which tests the snapshot guard, not app tolerance. So the property the gate is named for — the
  one that makes a rollout safe to run against live traffic at all — was asserted nowhere.

  It matters because convergence is deliberately **not** atomic. `ReconcileJob` walks the cold tail
  at `:reconcile_batch_size` per hour, so a fleet sits mixed for as long as that takes (measured at
  ~46k shards/hour on the rig, but a knob an operator can turn down). Every hour of that window, an
  unchanged Django app is issuing the same queries to tenants on both sides of the release. If a
  release could break the un-migrated side, expand-contract discipline would be worthless and the
  engine's whole "roll it gradually" design would be a liability rather than a feature.

  **These are not regression tests and must not be read as one.** They pass against the code as it
  already stands — the tolerance property was always true; what was missing was any assertion of it.
  That is exactly what a gate item without a test means: the behaviour is load-bearing, nothing
  proves it, and a future change could quietly take it away with the whole suite still green. These
  are the tripwire for that change.

  These drive the **real Hrana pipeline** (`Filo.Plug` → `Fathom.ShardExecutor` → the shard file),
  not `Migrator` internals, because the claim is about what a CLIENT sees mid-rollout.
  """
  use Fathom.DataCase, async: false

  import Plug.Test
  import Plug.Conn

  @moduletag :capture_log

  alias Fathom.{Directory, Migrator}

  @streams __MODULE__.Streams

  setup do
    start_supervised!({Filo.Streams, name: @streams})

    old_shard = "xver_old_#{System.unique_integer([:positive])}"
    new_shard = "xver_new_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for id <- [old_shard, new_shard] do
        Fathom.Shards.drain(id, 2_000)

        for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
            suffix <- ["", "-wal", "-shm", ".etag"],
            do: File.rm(Path.join(dir, "#{id}.db" <> suffix))
      end
    end)

    opts =
      Filo.Plug.init(
        executor: Fathom.ShardExecutor,
        streams: @streams,
        key: Filo.Baton.new_key(),
        open_arg: &Fathom.ShardExecutor.shard_from_conn/1
      )

    %{opts: opts, old_shard: old_shard, new_shard: new_shard}
  end

  defp sql(opts, shard, statement) do
    body = %{
      "baton" => nil,
      "requests" => [%{"type" => "execute", "stmt" => %{"sql" => statement}}]
    }

    conn(:post, "http://#{shard}.fathom.test/v3/pipeline", Jason.encode!(body))
    |> put_req_header("content-type", "application/json")
    |> Filo.Plug.call(opts)
  end

  defp rows(conn) do
    assert conn.status == 200
    [result] = Jason.decode!(conn.resp_body)["results"]
    assert result["type"] == "ok", "the wire returned an error: #{inspect(result)}"
    result["response"]["result"]["rows"]
  end

  test "a released vN does not break tenants still on vN-1", ctx do
    %{opts: opts, old_shard: old_shard, new_shard: new_shard} = ctx

    # Both tenants start at v1 with the SAME shape an app would have shipped against.
    for id <- [old_shard, new_shard] do
      {:ok, _} = Directory.resolve(id)
      rows(sql(opts, id, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)"))
      rows(sql(opts, id, "INSERT INTO app_thing (id, name) VALUES (1, 'alice')"))
      {:ok, _} = Directory.cutover(id, 1)
    end

    # v2 adds a nullable column — the expand half of expand-contract, i.e. the ONLY shape the
    # engine claims is safe to roll gradually.
    {:ok, _} = Migrator.release(2, "v2", ["ALTER TABLE app_thing ADD COLUMN created_at TEXT"])

    # One tenant converges; the other has not been swept yet. This is the mixed window.
    rows(sql(opts, new_shard, "ALTER TABLE app_thing ADD COLUMN created_at TEXT"))
    {:ok, _} = Directory.cutover(new_shard, 2)

    assert Migrator.head() == 2
    assert Migrator.status().laggards >= 1, "the fixture must actually be mid-rollout"

    # THE CLAIM. Old-schema queries — what a not-yet-deployed app version sends — must work against
    # BOTH sides. A release that broke this would take down every un-migrated tenant the moment it
    # landed, which is the failure expand-contract exists to prevent.
    for id <- [old_shard, new_shard] do
      assert [[%{"value" => "alice"}]] = rows(sql(opts, id, "SELECT name FROM app_thing"))
    end

    # And new-schema queries — what the deployed-after-convergence app sends — work on the migrated
    # side. The un-migrated side correctly does NOT have the column yet; that asymmetry is exactly
    # why the deploy gate (`converged == true`) exists, and asserting it here keeps the two halves
    # of the contract honest about each other.
    assert [[%{"type" => "null"}]] =
             rows(sql(opts, new_shard, "SELECT created_at FROM app_thing"))

    old_conn = sql(opts, old_shard, "SELECT created_at FROM app_thing")
    [result] = Jason.decode!(old_conn.resp_body)["results"]

    assert result["type"] == "error",
           "the un-migrated tenant must not silently answer a vN-only query"

    assert result["error"]["message"] =~ "created_at",
           "and the error must name the missing column, so a premature deploy is diagnosable"
  end

  test "writes keep working on both sides of the release", ctx do
    %{opts: opts, old_shard: old_shard, new_shard: new_shard} = ctx

    for id <- [old_shard, new_shard] do
      {:ok, _} = Directory.resolve(id)
      rows(sql(opts, id, "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)"))
      {:ok, _} = Directory.cutover(id, 1)
    end

    {:ok, _} = Migrator.release(2, "v2", ["ALTER TABLE app_thing ADD COLUMN created_at TEXT"])
    rows(sql(opts, new_shard, "ALTER TABLE app_thing ADD COLUMN created_at TEXT"))
    {:ok, _} = Directory.cutover(new_shard, 2)

    # A mixed fleet has to keep ACCEPTING WRITES on both sides, not merely reads — a rollout that
    # quietly turned un-migrated tenants read-only would be an outage for them.
    for id <- [old_shard, new_shard] do
      rows(sql(opts, id, "INSERT INTO app_thing (id, name) VALUES (2, 'bob')"))

      assert [[%{"value" => "bob"}]] =
               rows(sql(opts, id, "SELECT name FROM app_thing WHERE id = 2")),
             "the write did not land on #{id} — a mixed fleet must stay writable on BOTH sides"
    end
  end
end
