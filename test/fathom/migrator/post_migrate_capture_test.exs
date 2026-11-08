defmodule Fathom.Migrator.PostMigrateCaptureTest do
  @moduledoc """
  Expert review 2026-11-07 #53 at the `Capture` state machine: Django's `post_migrate` transactions
  (content types / permissions, no `django_migrations` row) used to park as "awaiting bookkeeping"
  DDL, get superseded by the next `BEGIN` with a noisy warning, and record nothing.

  Now (rewritten by `Fathom.Migrator.PostMigrate` upstream of Capture) they are ATTACHED to the
  version they follow — never minted as a version of their own, which would put `user_version` /
  `schema_version` ahead of Django's ledger.

  The statement stream is the VERBATIM one in `test/support/fixtures/post_migrate_capture.json`.
  """
  use Fathom.DataCase, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Directory, Migrator}
  alias Fathom.Migrator.{Capture, PostMigrate}

  @fixture "test/support/fixtures/post_migrate_capture.json"

  @alarms [
    [:fathom, :migrator, :capture_awaiting_dropped],
    [:fathom, :migrator, :post_migrate_unattached],
    [:fathom, :migrator, :migration_gap]
  ]

  setup do
    name = :"pm_capture_#{System.unique_integer([:positive])}"
    cap = start_supervised!(Supervisor.child_spec({Capture, name: name}, id: name))

    me = self()
    handler = "pm-capture-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      @alarms,
      fn event, meas, meta, _ -> send(me, {:alarm, List.last(event), meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    # The template at django_migrations count 15, version == count (so a captured version number is
    # also the ledger count, which is what the agreement assertions below rely on).
    {:ok, _} = Migrator.release(15, "baseline", [], 15)
    assert Migrator.next_version() == 16

    events = @fixture |> File.read!() |> Jason.decode!() |> Map.fetch!("events")
    %{cap: cap, events: events, conn: make_ref()}
  end

  # The migration's own transaction, verbatim from the fixture (events before the first `commit`).
  defp migrate!(cap, conn, events) do
    {migration, _rest} = Enum.split_while(events, &(&1["op"] != "commit"))

    for e <- migration do
      case e["op"] do
        "begin" -> Capture.begin(conn, e["count"], cap)
        "append" -> Capture.append(conn, e["sql"], e["args"], cap)
        "bookkeeping" -> Capture.bookkeeping(conn, e["sql"], e["args"], e["count"], cap)
      end
    end

    assert {:recorded, 16} = Capture.commit(conn, 16, cap)
  end

  # The verbatim post_migrate INSERTs, rewritten the way the executor's hook does it (the template's
  # own table resolves id 6 to widgets.gadget).
  defp post_migrate_inserts(events) do
    keys = %{6 => {"widgets", "gadget"}}

    events
    |> Enum.filter(fn e ->
      String.starts_with?(e["sql"] || "", ~s|INSERT INTO "django_content_type"|) or
        String.starts_with?(e["sql"] || "", ~s|INSERT INTO "auth_permission"|)
    end)
    |> Enum.map(fn e ->
      PostMigrate.capture_pairs(e["sql"], e["args"], fn ids -> Map.take(keys, ids) end)
    end)
  end

  # One post_migrate transaction: BEGIN, the writes, Django's `SELECT QUOTE(?)` echo, COMMIT.
  defp post_txn(cap, conn, pairs, count \\ 16) do
    Capture.begin(conn, count, cap)
    for {sql, args} <- pairs, do: Capture.append(conn, sql, args, cap)
    Capture.append(conn, "SELECT QUOTE(?)", ["echo"], cap)
    Capture.commit(conn, count, cap)
  end

  test "post_migrate transactions attach to the version they follow; no version is minted", ctx do
    %{cap: cap, events: events, conn: conn} = ctx
    [ct, perm] = post_migrate_inserts(events)

    log =
      capture_log(fn ->
        migrate!(cap, conn, events)

        assert {:attached, 16} = post_txn(cap, conn, ct)
        assert {:attached, 16} = post_txn(cap, conn, perm)
      end)

    # Still ONE new version, at the ledger count: the count invariant the outside-capture gap check
    # and `Ledger` rest on.
    assert Migrator.head() == 16
    assert Migrator.next_version() == 17
    assert Migrator.last_template_count() == 16
    refute log =~ "AWAITING"
    refute log =~ "OUTSIDE capture"
    refute_received {:alarm, _, _, _}

    # The migration's own statements, then the rewritten rows — in stream order, args aligned.
    [{pairs, nil}] = Map.values(Migrator.statement_steps([16]))
    [{create, _}, {bookkeeping, bk_args} | rows] = pairs

    assert create =~ ~s|CREATE TABLE "widgets_gadget"|
    assert Capture.bookkeeping?(bookkeeping)
    assert bk_args == ["widgets", "0002_gadget", "2026-11-08 07:13:34.656383"]
    assert length(rows) == 5
    assert Enum.all?(rows, fn {sql, _} -> PostMigrate.rewritten?(sql) end)

    assert [{_, ["widgets", "gadget", "widgets", "gadget"]} | perms] = rows

    assert Enum.map(perms, fn {_, [_name, code | _]} -> code end) ==
             ~w(add_gadget change_gadget delete_gadget view_gadget)

    # Attaching must not flag the version: the classifier knows exactly these shapes.
    assert Migrator.pending_review() == []
  end

  test "the AWAITING alarm is gone for post_migrate but still fires for superseded DDL", ctx do
    %{cap: cap, events: events, conn: conn} = ctx
    [ct, perm] = post_migrate_inserts(events)

    # (1) post_migrate-shaped transactions followed by a NEW BEGIN: silent.
    quiet =
      capture_log(fn ->
        migrate!(cap, conn, events)
        post_txn(cap, conn, ct)
        post_txn(cap, conn, perm)
        Capture.begin(conn, 16, cap)
        Capture.rollback(conn, cap)
        assert Capture.pending_count(cap) == 0
      end)

    refute quiet =~ "AWAITING"
    refute_received {:alarm, :capture_awaiting_dropped, _, _}

    # (2) CONTROL: genuinely superseded DDL (commit with no count rise, then a new BEGIN) alarms.
    other = make_ref()

    loud =
      capture_log(fn ->
        Capture.begin(other, 16, cap)
        Capture.append(other, "CREATE TABLE lost_ddl (id INTEGER)", [], cap)
        assert :noop = Capture.commit(other, 16, cap)
        Capture.begin(other, 16, cap)
        assert Capture.pending_count(cap) == 0
      end)

    assert loud =~ "AWAITING their django_migrations bookkeeping row"
    assert_received {:alarm, :capture_awaiting_dropped, %{count: 1}, _}
  end

  test "a RAW post_migrate INSERT (unresolved id) is not trusted: it parks and alarms as before",
       ctx do
    %{cap: cap, events: events, conn: conn} = ctx

    raw =
      Enum.find(events, &String.starts_with?(&1["sql"] || "", ~s|INSERT INTO "auth_permission"|))

    migrate!(cap, conn, events)

    loud =
      capture_log(fn ->
        Capture.begin(conn, 16, cap)
        Capture.append(conn, raw["sql"], raw["args"], cap)
        assert :noop = Capture.commit(conn, 16, cap)
        Capture.begin(conn, 16, cap)
        # The warning is emitted by the BEGIN cast; a call syncs behind it.
        assert Capture.pending_count(cap) == 0
      end)

    assert loud =~ "AWAITING"
    assert_received {:alarm, :capture_awaiting_dropped, _, _}
    assert length(Migrator.statements(16)) == 2
  end

  test "refused LOUDLY once a tenant has crossed the version: the rows are not attached", ctx do
    %{cap: cap, events: events, conn: conn} = ctx
    [ct, _perm] = post_migrate_inserts(events)
    migrate!(cap, conn, events)

    # A tenant that already migrated onto v16 before the rows arrived would never receive them.
    tenant = "pmcross#{System.unique_integer([:positive])}"
    {:ok, _} = Directory.resolve(tenant)
    {:ok, _} = Directory.cutover(tenant, 16)
    before = Migrator.statements(16)

    log =
      capture_log(fn -> assert {:error, {:already_rolled_out, 1}} = post_txn(cap, conn, ct) end)

    assert log =~ "could NOT be attached"
    assert_received {:alarm, :post_migrate_unattached, %{count: 1}, %{template_count: 16}}
    assert Migrator.statements(16) == before
  end

  test "rows that follow nothing recorded are refused, not attached to the wrong version", ctx do
    %{cap: cap, events: events, conn: conn} = ctx
    [ct, _perm] = post_migrate_inserts(events)

    # The template says count 99; the newest release was recorded at 15.
    log =
      capture_log(fn ->
        assert {:error, {:count_mismatch, 15, 15, 99}} = post_txn(cap, conn, ct, 99)
      end)

    assert log =~ "could NOT be attached"
    assert_received {:alarm, :post_migrate_unattached, _, %{template_count: 99}}
    assert Migrator.statements(15) == []
  end
end
