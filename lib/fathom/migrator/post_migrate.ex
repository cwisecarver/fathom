defmodule Fathom.Migrator.PostMigrate do
  @moduledoc """
  Rewrites Django's `post_migrate` writes into a form that replays safely onto any tenant (expert
  review 2026-11-07 #53).

  `manage.py migrate` ends by emitting `post_migrate`. `django.contrib.contenttypes` and
  `django.contrib.auth` answer it with one `bulk_create` per table, each its own `BEGIN … COMMIT` on
  the template stream:

      INSERT INTO "django_content_type" ("app_label", "model") VALUES (?, ?) RETURNING "django_content_type"."id"
      INSERT INTO "auth_permission" ("name", "content_type_id", "codename") VALUES (?, ?, ?), … RETURNING "auth_permission"."id"

  Captured verbatim they are unsafe to replay: `content_type_id` is bound as the TEMPLATE's literal
  row id, and a tenant's `django_content_type` ids differ from the template's whenever its rows were
  created in a different order (a tenant that took the manual `manage.py migrate` remedy, a
  different app set). A literal id replayed onto such a tenant attaches the permission to the WRONG
  content type — silently. So the capture hook resolves each bound id back to its natural key
  `(app_label, model)` against the template's own table and records these two idempotent shapes
  instead:

      INSERT INTO "django_content_type" ("app_label", "model") SELECT ?, ? WHERE NOT EXISTS (… app_label = ? AND model = ?)
      INSERT INTO "auth_permission" ("name", "content_type_id", "codename")
        SELECT ?, "ct"."id", ? FROM "django_content_type" AS "ct" WHERE app_label = ? AND model = ?
        AND NOT EXISTS (… content_type_id = "ct"."id" AND codename = ?)

  Both are deterministic and idempotent: a tenant that already has the row (forked from a newer
  template, or one that took the manual remedy) is left untouched, and a tenant with different ids
  resolves to ITS OWN `content_type.id`. They are bound statements like any other, so
  `Fathom.Migrator.Copy` replays them with no change.

  Recognition is exact: only these two column lists, with `n` value groups matching `n` rows' worth of
  bound args, with or without Django's `RETURNING` clause. Any other shape, a missing content type
  id, or a non-text value is left RAW — and a raw `INSERT … VALUES` is still flagged by
  `Fathom.Migrator.Capture.data_migration_statements/1` / parked by Capture, i.e. it fails loud, not
  open.
  """

  require Logger

  @ct_head ~s|INSERT INTO "django_content_type" ("app_label", "model") VALUES |
  @ct_returning ~s| RETURNING "django_content_type"."id"|
  @perm_head ~s|INSERT INTO "auth_permission" ("name", "content_type_id", "codename") VALUES |
  @perm_returning ~s| RETURNING "auth_permission"."id"|

  @content_type_sql ~s|INSERT INTO "django_content_type" ("app_label", "model") | <>
                      ~s|SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM "django_content_type" | <>
                      ~s|WHERE "app_label" = ? AND "model" = ?)|

  @permission_sql ~s|INSERT INTO "auth_permission" ("name", "content_type_id", "codename") | <>
                    ~s|SELECT ?, "ct"."id", ? FROM "django_content_type" AS "ct" | <>
                    ~s|WHERE "ct"."app_label" = ? AND "ct"."model" = ? AND NOT EXISTS | <>
                    ~s|(SELECT 1 FROM "auth_permission" AS "p" WHERE "p"."content_type_id" = "ct"."id" | <>
                    ~s|AND "p"."codename" = ?)|

  @type kind :: :content_type | :permission
  @type pair :: {String.t(), [term()]}

  @doc """
  Which post_migrate INSERT shape `sql` is with `args` bound, or `nil`. Exact text match against the
  SQL Django 5.0 emits for `n` rows — `n` is read off the bound-arg count, so a statement whose
  placeholder groups disagree with its args is not recognized.
  """
  @spec shape(String.t(), [term()]) :: kind() | nil
  def shape(<<"INSERT INTO \"", _::binary>> = sql, args) when is_list(args) do
    cond do
      matches?(sql, @ct_head, @ct_returning, "(?, ?)", length(args), 2) -> :content_type
      matches?(sql, @perm_head, @perm_returning, "(?, ?, ?)", length(args), 3) -> :permission
      true -> nil
    end
  end

  def shape(_sql, _args), do: nil

  defp matches?(sql, head, returning, group, nargs, arity) do
    rows = div(nargs, arity)
    values = head <> Enum.join(List.duplicate(group, rows), ", ")

    rows > 0 and rem(nargs, arity) == 0 and (sql == values or sql == values <> returning)
  end

  @doc """
  Whether `sql` is exactly one of the two REWRITTEN statements this module emits — what
  `Fathom.Migrator.Capture` treats as "a post_migrate transaction" and what
  `data_migration_statements/1` is taught to accept.
  """
  @spec rewritten?(term()) :: boolean()
  def rewritten?(sql), do: sql == @content_type_sql or sql == @permission_sql

  @doc """
  The `{sql, args}` pairs to buffer for a template-stream statement.

  A non-post_migrate statement, or one that cannot be rewritten safely, comes back unchanged as
  `[{sql, args}]`. A recognized INSERT comes back as one rewritten pair per row. `lookup` maps a list
  of `django_content_type.id`s to `%{id => {app_label, model}}` against the TEMPLATE connection (the
  caller owns the connection); it is only invoked for the permission shape.
  """
  @spec capture_pairs(String.t(), [term()], ([integer()] ->
                                               %{integer() => {String.t(), String.t()}})) ::
          [pair()]
  def capture_pairs(sql, args, lookup) do
    with kind when kind != nil <- shape(sql, args),
         {:ok, pairs} <- rewrite(kind, args, lookup) do
      pairs
    else
      _ -> [{sql, args}]
    end
  end

  # One rewritten statement per row, in bound order. Row-at-a-time keeps each statement's args
  # independent (a failed row cannot misalign its neighbours) and costs nothing at replay.
  defp rewrite(:content_type, args, _lookup) do
    rows = Enum.chunk_every(args, 2)

    if Enum.all?(rows, fn [a, m] -> is_binary(a) and is_binary(m) end),
      do: {:ok, Enum.map(rows, fn [a, m] -> {@content_type_sql, [a, m, a, m]} end)},
      else: :error
  end

  defp rewrite(:permission, args, lookup) do
    rows = Enum.chunk_every(args, 3)
    ids = rows |> Enum.map(fn [_name, id, _code] -> id end) |> Enum.uniq()

    if Enum.all?(ids, &is_integer/1) do
      keys = lookup.(ids)

      rows
      |> Enum.reduce_while([], fn [name, id, code], acc ->
        with true <- is_binary(name) and is_binary(code),
             {app, model} <- Map.get(keys, id) do
          {:cont, [{@permission_sql, [name, code, app, model, code]} | acc]}
        else
          _ ->
            Logger.warning(
              "post_migrate: cannot resolve auth_permission content_type_id #{inspect(id)} on the " <>
                "template to (app_label, model); the statement is captured RAW and will be held " <>
                "for review rather than replayed with a template-literal id"
            )

            {:halt, :error}
        end
      end)
      |> case do
        :error -> :error
        acc -> {:ok, Enum.reverse(acc)}
      end
    else
      :error
    end
  end

  @doc "The content-type resolution query for `ids` (one `?` per id), for the capture hook's connection."
  @spec lookup_sql([integer()]) :: String.t()
  def lookup_sql(ids) do
    marks = ids |> Enum.map(fn _ -> "?" end) |> Enum.join(", ")
    ~s|SELECT "id", "app_label", "model" FROM "django_content_type" WHERE "id" IN (#{marks})|
  end
end
