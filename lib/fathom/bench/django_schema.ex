defmodule Fathom.Bench.DjangoSchema do
  @moduledoc """
  A Django-sized, wide-schema SQLite fixture for the `served_django_*` bench arm (expert review
  2026-10-21 #18a).

  `Fathom.Keystone` (6 tables / 4 indexes) is what every density number was measured on, but each
  SQLite connection parses and holds its OWN copy of the schema (`SQLITE_DBSTATUS_SCHEMA_USED`;
  shared cache is off), so per-connection memory grows with the tenant's schema. A real Django
  tenant carries ~12 framework tables (auth, contenttypes, sessions, admin, sites) before its own
  models. This builds the same shape synthetically: the 12 contrib tables with the indexes
  `manage.py migrate` creates, plus `app_model_<i>` tables with the columns and index mix Django
  emits for a typical model (an auto id, two datetimes, a FK column, a status, a decimal, text),
  default 45 tables / ~100 indexes in total.

  Synthetic on purpose: a real `manage.py migrate` needs Python and the django-libsql driver, and
  the memory cost depends on the parsed schema's size, not on who generated the DDL.
  """

  alias Fathom.Shard.Connection

  @default_tables 45

  # {table, column definitions, [extra index DDL]}. The FK / unique indexes mirror what Django emits
  # (it creates one index per FK column and one per `unique=True` / `unique_together`).
  @contrib [
    {"django_migrations",
     "id integer NOT NULL PRIMARY KEY AUTOINCREMENT, app varchar(255) NOT NULL, name varchar(255) NOT NULL, applied datetime NOT NULL",
     []},
    {"django_content_type",
     "id integer NOT NULL PRIMARY KEY AUTOINCREMENT, app_label varchar(100) NOT NULL, model varchar(100) NOT NULL",
     [
       "CREATE UNIQUE INDEX django_content_type_app_label_model_uniq ON django_content_type (app_label, model)"
     ]},
    {"auth_permission",
     "id integer NOT NULL PRIMARY KEY AUTOINCREMENT, content_type_id integer NOT NULL REFERENCES django_content_type (id), codename varchar(100) NOT NULL, name varchar(255) NOT NULL",
     [
       "CREATE UNIQUE INDEX auth_permission_content_type_id_codename_uniq ON auth_permission (content_type_id, codename)",
       "CREATE INDEX auth_permission_content_type_id_idx ON auth_permission (content_type_id)"
     ]},
    {"auth_group",
     "id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name varchar(150) NOT NULL UNIQUE", []},
    {"auth_group_permissions",
     "id integer NOT NULL PRIMARY KEY AUTOINCREMENT, group_id integer NOT NULL REFERENCES auth_group (id), permission_id integer NOT NULL REFERENCES auth_permission (id)",
     [
       "CREATE UNIQUE INDEX auth_group_permissions_group_id_permission_id_uniq ON auth_group_permissions (group_id, permission_id)",
       "CREATE INDEX auth_group_permissions_group_id_idx ON auth_group_permissions (group_id)",
       "CREATE INDEX auth_group_permissions_permission_id_idx ON auth_group_permissions (permission_id)"
     ]},
    {"auth_user",
     "id integer NOT NULL PRIMARY KEY AUTOINCREMENT, password varchar(128) NOT NULL, last_login datetime NULL, is_superuser bool NOT NULL, username varchar(150) NOT NULL UNIQUE, last_name varchar(150) NOT NULL, email varchar(254) NOT NULL, is_staff bool NOT NULL, is_active bool NOT NULL, date_joined datetime NOT NULL, first_name varchar(150) NOT NULL",
     []},
    {"auth_user_groups",
     "id integer NOT NULL PRIMARY KEY AUTOINCREMENT, user_id integer NOT NULL REFERENCES auth_user (id), group_id integer NOT NULL REFERENCES auth_group (id)",
     [
       "CREATE UNIQUE INDEX auth_user_groups_user_id_group_id_uniq ON auth_user_groups (user_id, group_id)",
       "CREATE INDEX auth_user_groups_user_id_idx ON auth_user_groups (user_id)",
       "CREATE INDEX auth_user_groups_group_id_idx ON auth_user_groups (group_id)"
     ]},
    {"auth_user_user_permissions",
     "id integer NOT NULL PRIMARY KEY AUTOINCREMENT, user_id integer NOT NULL REFERENCES auth_user (id), permission_id integer NOT NULL REFERENCES auth_permission (id)",
     [
       "CREATE UNIQUE INDEX auth_user_user_permissions_user_id_permission_id_uniq ON auth_user_user_permissions (user_id, permission_id)",
       "CREATE INDEX auth_user_user_permissions_user_id_idx ON auth_user_user_permissions (user_id)",
       "CREATE INDEX auth_user_user_permissions_permission_id_idx ON auth_user_user_permissions (permission_id)"
     ]},
    {"django_admin_log",
     "id integer NOT NULL PRIMARY KEY AUTOINCREMENT, object_id text NULL, object_repr varchar(200) NOT NULL, action_flag smallint unsigned NOT NULL, change_message text NOT NULL, content_type_id integer NULL REFERENCES django_content_type (id), user_id integer NOT NULL REFERENCES auth_user (id), action_time datetime NOT NULL",
     [
       "CREATE INDEX django_admin_log_content_type_id_idx ON django_admin_log (content_type_id)",
       "CREATE INDEX django_admin_log_user_id_idx ON django_admin_log (user_id)"
     ]},
    {"django_session",
     "session_key varchar(40) NOT NULL PRIMARY KEY, session_data text NOT NULL, expire_date datetime NOT NULL",
     ["CREATE INDEX django_session_expire_date_idx ON django_session (expire_date)"]},
    {"django_site",
     "id integer NOT NULL PRIMARY KEY AUTOINCREMENT, domain varchar(100) NOT NULL UNIQUE, name varchar(50) NOT NULL",
     []},
    {"authtoken_token",
     "key varchar(40) NOT NULL PRIMARY KEY, created datetime NOT NULL, user_id integer NOT NULL UNIQUE REFERENCES auth_user (id)",
     []}
  ]

  @doc "The table the served arm reads from (it is seeded with rows)."
  @spec probe_table() :: String.t()
  def probe_table, do: "app_model_1"

  @wide_cols 29

  @doc "The 29-column table added by `build!(path, wide: true)` (the Django request arm)."
  @spec wide_table() :: String.t()
  def wide_table, do: "app_wide"

  @doc """
  A Django-shaped point SELECT over `app_wide`: every one of its #{@wide_cols} columns, each as a
  qualified `"app_wide"."col"`, one `?` for the id, `LIMIT 21` (what `.get()` emits).
  """
  @spec wide_select_sql() :: String.t()
  def wide_select_sql do
    cols = Enum.map_join(wide_columns(), ", ", &~s("app_wide"."#{&1}"))
    ~s(SELECT #{cols} FROM "app_wide" WHERE "app_wide"."id" = ? LIMIT 21)
  end

  @doc """
  A Django-shaped three-way join (`select_related`): `app_wide` -> `auth_user` -> `app_model_1`,
  36 selected columns (the 29 wide ones, 3 from the user, 4 from the model), one `?`.
  """
  @spec join_sql() :: String.t()
  def join_sql do
    wide = Enum.map_join(wide_columns(), ", ", &~s("app_wide"."#{&1}"))

    ~s|SELECT #{wide}, "auth_user"."username", "auth_user"."email", "auth_user"."is_active", | <>
      ~s|"app_model_1"."name", "app_model_1"."status", "app_model_1"."amount", "app_model_1"."notes" | <>
      ~s|FROM "app_wide" INNER JOIN "auth_user" ON ("app_wide"."owner_id" = "auth_user"."id") | <>
      ~s|INNER JOIN "app_model_1" ON ("app_model_1"."owner_id" = "auth_user"."id") | <>
      ~s|WHERE "app_wide"."id" = ? LIMIT 21|
  end

  defp wide_columns, do: ["id", "owner_id" | Enum.map(1..(@wide_cols - 2), &"f#{&1}")]

  @doc """
  Creates the schema in the SQLite file at `path` (using its own connection, closed before this
  returns) and seeds `app_model_1` with `rows` rows. Returns the counted `%{tables, indexes}` so the
  caller can assert the fixture is the size it claims.
  """
  @spec build!(Path.t(), keyword()) :: %{tables: pos_integer(), indexes: pos_integer()}
  def build!(path, opts \\ []) do
    tables = Keyword.get(opts, :tables, @default_tables)
    rows = Keyword.get(opts, :rows, 50)
    app_tables = max(tables - length(@contrib), 1)

    {:ok, conn} = Connection.open(path)

    try do
      ddl =
        Enum.flat_map(@contrib, fn {name, cols, idx} ->
          ["CREATE TABLE #{name} (#{cols})" | idx]
        end) ++ Enum.flat_map(1..app_tables, &app_model_ddl/1)

      {:ok, _} = Connection.query(conn, "BEGIN", [])
      Enum.each(ddl, fn sql -> {:ok, _} = Connection.query(conn, sql, []) end)

      {:ok, _} =
        Connection.query(
          conn,
          "INSERT INTO auth_user (id, password, is_superuser, username, last_name, email, is_staff, is_active, date_joined, first_name) " <>
            "VALUES (1, 'x', 0, 'admin', 'a', 'a@example.com', 1, 1, '2026-10-21 00:00:00', 'a')",
          []
        )

      for i <- 1..rows do
        {:ok, _} =
          Connection.query(
            conn,
            "INSERT INTO #{probe_table()} (created_at, updated_at, name, owner_id, status, amount, notes, home_id) " <>
              "VALUES ('2026-10-21 00:00:00', '2026-10-21 00:00:00', ?, 1, 'active', '1.50', 'note', ?)",
            ["row-#{i}", rem(i, 7)]
          )
      end

      if Keyword.get(opts, :wide, false), do: build_wide!(conn, rows)

      {:ok, _} = Connection.query(conn, "COMMIT", [])

      count = fn type ->
        {:ok, %{rows: [[n]]}} =
          Connection.query(
            conn,
            "SELECT count(*) FROM sqlite_master WHERE type = ? AND name NOT LIKE 'sqlite_%'",
            [type]
          )

        n
      end

      %{tables: count.("table"), indexes: count.("index")}
    after
      Connection.close(conn)
    end
  end

  # The 29-column model of the Django request arm (opt-in: `served_django` keeps its table count).
  defp build_wide!(conn, rows) do
    fields = Enum.map_join(1..(@wide_cols - 2), ", ", &"f#{&1} varchar(40) NOT NULL")

    {:ok, _} =
      Connection.query(
        conn,
        "CREATE TABLE app_wide (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, " <>
          "owner_id integer NOT NULL REFERENCES auth_user (id), #{fields})",
        []
      )

    {:ok, _} =
      Connection.query(conn, "CREATE INDEX app_wide_owner_id_idx ON app_wide (owner_id)", [])

    names = Enum.map_join(1..(@wide_cols - 2), ", ", &"f#{&1}")
    marks = Enum.map_join(1..(@wide_cols - 2), ", ", fn _ -> "'v'" end)

    for _ <- 1..rows do
      {:ok, _} =
        Connection.query(
          conn,
          "INSERT INTO app_wide (owner_id, #{names}) VALUES (1, #{marks})",
          []
        )
    end

    :ok
  end

  # One generated Django model: an auto id, two datetimes, a name, a FK, a status, a decimal, text
  # and a nullable "home" key; indexes on the FK, on (home, created) and on the status.
  defp app_model_ddl(i) do
    t = "app_model_#{i}"

    [
      "CREATE TABLE #{t} (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, created_at datetime NOT NULL, " <>
        "updated_at datetime NOT NULL, name varchar(255) NOT NULL, owner_id integer NOT NULL REFERENCES auth_user (id), " <>
        "status varchar(20) NOT NULL, amount decimal NOT NULL, notes text NOT NULL, home_id integer NULL)",
      "CREATE INDEX #{t}_owner_id_idx ON #{t} (owner_id)",
      "CREATE INDEX #{t}_home_id_created_at_idx ON #{t} (home_id, created_at)",
      "CREATE INDEX #{t}_status_idx ON #{t} (status)"
    ]
  end
end
