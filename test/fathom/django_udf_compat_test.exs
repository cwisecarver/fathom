defmodule Fathom.DjangoUdfCompatTest do
  @moduledoc """
  Expert review 2026-08-01 **#19**: Django's SQLite backend registers ~20 Python UDFs on every
  *client* connection. Under `django-libsql` the SQL crosses the wire and is compiled by
  **fathom's** SQLite, where those functions do not exist — so ordinary querysets
  (`__year`, `__date`, `__regex`, `Trunc*`, `F()` arithmetic on a `DurationField`) raise
  `OperationalError` against the flagship "point an unchanged Django app at it" claim, while basic
  CRUD works. That asymmetry is exactly why the working slice and both TPC harnesses never caught
  it: TPC-B and TPC-C use only builtins.

  **This file is the tracked list, not the fix.** The finding's own order of work puts it first —
  "this alone converts an unknown-unknown into a tracked list" — and the fix it recommends next
  ("implement the pure ones via exqlite's scalar-function registration") **cannot be done as
  written**: exqlite 0.37.0 exposes no user-defined-function API at all. There is no
  `create_function`, no scalar/aggregate registration, in either `Exqlite.Sqlite3` or
  `Exqlite.Sqlite3NIF`. See `docs/quickstart-django.md` for the options and
  `tasks/todo.md` for the decision that is actually pending.

  So these tests assert **what is true today**, deliberately. Each unsupported function is pinned as
  unsupported. When one is implemented its test FAILS — and that is the intent: the failure is the
  reminder to move it into the supported list and update the operator doc. A list that silently
  stayed green as reality changed underneath it would be worth nothing.

  The SQL shapes here are the ones Django's SQLite backend actually emits (`django/db/backends/
  sqlite3/operations.py`), not invented approximations — a probe that tested a function fathom will
  never be asked for would prove nothing.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  # The FULL inventory Django registers (django/db/backends/sqlite3/_functions.py, ~600 lines),
  # not the subset the finding happened to name. Probed 2026-08-05: 19 of 54 already resolve.
  #
  # Split by what it would take to supply the rest, because "45 missing functions" and "6 hard
  # ones" are very different projects and the raw count hides which:
  #
  #   * MATH is free — this SQLite is built with SQLITE_ENABLE_MATH_FUNCTIONS, so all 18 resolve
  #     today. Worth pinning: a future build without that flag silently breaks Django's Power/Log/
  #     trig lookups, and nothing else would catch it.
  #   * EASY — pure computation with an obvious implementation (hashes, padding, bit ops, uuid).
  #   * MODERATE — date arithmetic, no timezone involved.
  #   * TZ — needs a timezone DATABASE server-side (`tzname`/`conn_tzname` go through zoneinfo).
  #     These are the architectural decision the finding said "actually lives here" — AND they are
  #     the ones ordinary apps hit first (`__year`, `__date`, `TruncMonth`), so the cheap 29 do not
  #     buy off the headline breakage.
  @supported_today [
    {"ACOS", "SELECT ACOS(0.5)"},
    {"ASIN", "SELECT ASIN(0.5)"},
    {"ATAN", "SELECT ATAN(0.5)"},
    {"ATAN2", "SELECT ATAN2(1,1)"},
    {"CEILING", "SELECT CEILING(1.2)"},
    {"COS", "SELECT COS(0)"},
    {"DEGREES", "SELECT DEGREES(1)"},
    {"EXP", "SELECT EXP(1)"},
    {"FLOOR", "SELECT FLOOR(1.2)"},
    {"LN", "SELECT LN(1)"},
    {"LOG", "SELECT LOG(2,8)"},
    {"MOD", "SELECT MOD(5,2)"},
    {"PI", "SELECT PI()"},
    {"POWER", "SELECT POWER(2,3)"},
    {"RADIANS", "SELECT RADIANS(90)"},
    {"SIN", "SELECT SIN(0)"},
    {"SQRT", "SELECT SQRT(4)"},
    {"TAN", "SELECT TAN(0)"},
    {"SIGN", "SELECT SIGN(-2)"}
  ]

  @missing_easy [
    {"LPAD", "SELECT LPAD('x',3,'0')"},
    {"RPAD", "SELECT RPAD('x',3,'0')"},
    {"REPEAT", "SELECT REPEAT('x',3)"},
    {"REVERSE", "SELECT REVERSE('abc')"},
    {"MD5", "SELECT MD5('x')"},
    {"SHA1", "SELECT SHA1('x')"},
    {"SHA224", "SELECT SHA224('x')"},
    {"SHA256", "SELECT SHA256('x')"},
    {"SHA384", "SELECT SHA384('x')"},
    {"SHA512", "SELECT SHA512('x')"},
    {"regexp", "SELECT 'abc' REGEXP '^a'"},
    {"BITXOR", "SELECT BITXOR(1,2)"},
    {"COT", "SELECT COT(1)"},
    {"UUIDV4", "SELECT UUIDV4()"},
    {"UUIDV7", "SELECT UUIDV7()"},
    {"RAND", "SELECT RAND()"},
    {"STDDEV_POP", "SELECT STDDEV_POP(v) FROM (SELECT 1 AS v UNION SELECT 3)"},
    {"STDDEV_SAMP", "SELECT STDDEV_SAMP(v) FROM (SELECT 1 AS v UNION SELECT 3)"},
    {"VAR_POP", "SELECT VAR_POP(v) FROM (SELECT 1 AS v UNION SELECT 3)"},
    {"VAR_SAMP", "SELECT VAR_SAMP(v) FROM (SELECT 1 AS v UNION SELECT 3)"},
    {"ANY_VALUE", "SELECT ANY_VALUE(v) FROM (SELECT 1 AS v UNION SELECT 3)"},
    {"BIT_AND", "SELECT BIT_AND(v) FROM (SELECT 1 AS v UNION SELECT 3)"},
    {"BIT_OR", "SELECT BIT_OR(v) FROM (SELECT 1 AS v UNION SELECT 3)"},
    {"BIT_XOR", "SELECT BIT_XOR(v) FROM (SELECT 1 AS v UNION SELECT 3)"}
  ]

  @missing_moderate [
    {"django_time_extract", "SELECT django_time_extract('hour','12:00:00')"},
    {"django_time_trunc", "SELECT django_time_trunc('hour','12:00:00',NULL,NULL)"},
    {"django_time_diff", "SELECT django_time_diff('12:00:00','11:00:00')"},
    {"django_timestamp_diff",
     "SELECT django_timestamp_diff('2026-08-05 12:00:00','2026-08-05 11:00:00')"},
    {"django_format_dtdelta", "SELECT django_format_dtdelta('+',1,2)"}
  ]

  @missing_tz [
    {"django_date_extract", "SELECT django_date_extract('year','2026-08-05')"},
    {"django_date_trunc", "SELECT django_date_trunc('month','2026-08-05',NULL,NULL)"},
    {"django_datetime_cast_date",
     "SELECT django_datetime_cast_date('2026-08-05 12:00:00',NULL,NULL)"},
    {"django_datetime_cast_time",
     "SELECT django_datetime_cast_time('2026-08-05 12:00:00',NULL,NULL)"},
    {"django_datetime_extract",
     "SELECT django_datetime_extract('hour','2026-08-05 12:00:00',NULL,NULL)"},
    {"django_datetime_trunc",
     "SELECT django_datetime_trunc('day','2026-08-05 12:00:00',NULL,NULL)"}
  ]

  @missing @missing_easy ++ @missing_moderate ++ @missing_tz

  # Builtins Django also leans on. These MUST work — if one of these ever regresses, the problem is
  # not a missing UDF, it is the engine, and the two failures look identical from Django's side.
  @builtins [
    {"LOWER", "SELECT LOWER('ABC')"},
    {"strftime", "SELECT strftime('%Y', '2026-08-05')"},
    {"json_extract", "SELECT json_extract('{\"a\":1}', '$.a')"},
    {"ABS", "SELECT ABS(-1)"}
  ]

  setup do
    path =
      Path.join(System.tmp_dir!(), "fathom_udf_#{System.unique_integer([:positive])}.db")

    {:ok, conn} = Connection.open(path)

    on_exit(fn ->
      Connection.close(conn)
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    end)

    %{conn: conn}
  end

  defp available?(conn, sql) do
    case Connection.query(conn, sql, []) do
      {:ok, _} -> true
      {:error, reason} -> not (inspect(reason) =~ "no such function")
    end
  end

  test "the builtins Django relies on all resolve", %{conn: conn} do
    for {name, sql} <- @builtins do
      assert available?(conn, sql),
             "builtin #{name} is missing — that is an engine problem, not #19"
    end
  end

  # Free today, and silently losable. SQLite only has these when compiled with
  # SQLITE_ENABLE_MATH_FUNCTIONS; a future exqlite/build change that drops the flag would break
  # Django's Power/Log/trig lookups with no other signal.
  test "the 19 functions Django needs that SQLite already provides still resolve", %{conn: conn} do
    missing = for {name, sql} <- @supported_today, not available?(conn, sql), do: name

    assert missing == [],
           "these used to resolve and no longer do: #{inspect(missing)} — check whether the " <>
             "SQLite build lost SQLITE_ENABLE_MATH_FUNCTIONS"
  end

  # The tracked list, pinned as UNSUPPORTED on purpose: when one starts working this test fails,
  # which is the prompt to move it into @supported_today, add a test asserting the RESULT (a UDF
  # returning the wrong value is worse than a missing one — Django will not raise), and update the
  # table in docs/quickstart-django.md.
  test "the 35 still-missing Django functions (#19 — move each here as it lands)", %{conn: conn} do
    now_supported = for {name, sql} <- @missing, available?(conn, sql), do: name

    assert now_supported == [],
           """
           These now resolve server-side: #{inspect(now_supported)}.

           Move them to @supported_today, assert their RESULTS in a behavioural test, and update
           docs/quickstart-django.md.
           """
  end

  # The scoping number, asserted so it cannot drift silently in a doc. 24 easy + 5 moderate is
  # plausibly a weekend; the 6 tz-dependent ones need a timezone database server-side and are the
  # actual project — and they are also the ones ordinary apps hit first.
  test "the effort split is 24 easy / 5 moderate / 6 timezone-dependent" do
    assert length(@missing_easy) == 24
    assert length(@missing_moderate) == 5
    assert length(@missing_tz) == 6
    assert length(@supported_today) + length(@missing) == 54
  end

  # The operator-visible half. `no such function` is what reaches Django, and it surfaces as an
  # OperationalError that reads like a client bug — the finding's actual complaint. Pinning the
  # error text keeps the doc's triage section honest about what an operator will actually see.
  test "an unsupported lookup fails with `no such function`, the string operators will report", %{
    conn: conn
  } do
    assert {:error, reason} =
             Connection.query(conn, "SELECT django_date_extract('year', '2026-08-05')", [])

    assert inspect(reason) =~ "no such function",
           "the failure an operator sees changed shape; docs/quickstart-django.md quotes it"
  end
end
