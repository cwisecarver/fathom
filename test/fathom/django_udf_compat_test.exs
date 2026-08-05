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

  # Django emits these on ordinary lookups. Value is a representative call as Django writes it.
  @django_udfs [
    {"django_date_extract", "SELECT django_date_extract('year', '2026-08-05')"},
    {"django_date_trunc", "SELECT django_date_trunc('month', '2026-08-05', NULL, NULL)"},
    {"django_datetime_cast_date",
     "SELECT django_datetime_cast_date('2026-08-05 12:00:00', NULL, NULL)"},
    {"django_datetime_cast_time",
     "SELECT django_datetime_cast_time('2026-08-05 12:00:00', NULL, NULL)"},
    {"django_datetime_extract",
     "SELECT django_datetime_extract('hour', '2026-08-05 12:00:00', NULL, NULL)"},
    {"django_datetime_trunc",
     "SELECT django_datetime_trunc('day', '2026-08-05 12:00:00', NULL, NULL)"},
    {"django_time_extract", "SELECT django_time_extract('hour', '12:00:00')"},
    {"django_time_trunc", "SELECT django_time_trunc('hour', '12:00:00', NULL, NULL)"},
    {"django_time_diff", "SELECT django_time_diff('12:00:00', '11:00:00')"},
    {"django_timestamp_diff",
     "SELECT django_timestamp_diff('2026-08-05 12:00:00', '2026-08-05 11:00:00')"},
    {"django_format_dtdelta", "SELECT django_format_dtdelta('+', 1, 2)"},
    {"django_power", "SELECT django_power(2, 8)"},
    {"regexp", "SELECT 'abc' REGEXP '^a'"},
    {"LPAD", "SELECT LPAD('x', 3, '0')"},
    {"RPAD", "SELECT RPAD('x', 3, '0')"},
    {"REPEAT", "SELECT REPEAT('x', 3)"},
    {"STDDEV_POP", "SELECT STDDEV_POP(v) FROM (SELECT 1 AS v UNION SELECT 3)"},
    {"VAR_POP", "SELECT VAR_POP(v) FROM (SELECT 1 AS v UNION SELECT 3)"}
  ]

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

  # The tracked list. Pinned as UNSUPPORTED on purpose: when one starts working this test fails,
  # which is the prompt to move it to the supported set and update docs/quickstart-django.md.
  test "every Django UDF is still unsupported server-side (#19 — update this list when fixed)", %{
    conn: conn
  } do
    supported = for {name, sql} <- @django_udfs, available?(conn, sql), do: name

    assert supported == [],
           """
           These Django UDFs now resolve server-side: #{inspect(supported)}.

           That is good news, and this test is the tracked list that has to move with it:
             1. drop them from @django_udfs here,
             2. add a real behavioural test asserting the RESULT (not merely that the function
                exists — a UDF returning the wrong value is worse than a missing one, because
                Django will not raise),
             3. update the unsupported-lookup table in docs/quickstart-django.md.
           """
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
