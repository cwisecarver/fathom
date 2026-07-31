defmodule Fathom.Keystone do
  @moduledoc """
  The canonical **keystone** database for fathom's tests and benchmarks: one SQLite file that
  exercises every SQLite storage class and type affinity, with deterministically fuzzed data.

  A keystone is the template a tenant is forked from and the source a migration copies, so it is
  the shape every fidelity claim in fathom rests on. The seeds it replaces did not carry that
  weight: `Fathom.Bench.copy_throughput/1` measured a three-column `(INTEGER, TEXT, INTEGER)`
  table and `ForkTest.seed_template!/1` two small ones. Between them they touched **two of
  SQLite's five storage classes**. A copy, fork, flush, or pull that silently corrupted a BLOB, a
  negative zero, a NULL, or a 64-bit integer at its boundary would have passed every one of them.

  ## What it covers

    * **All five storage classes** — NULL, INTEGER, REAL, TEXT, BLOB.
    * **All five type affinities**, spelled through the declared-type rules in SQLite's
      "Determination of Column Affinity": INTEGER (`INT`, `BIGINT`, `INT8`, …), TEXT
      (`VARCHAR(255)`, `CLOB`, `NCHAR(55)`, …), BLOB (`BLOB`, and a column with **no** declared
      type), REAL (`REAL`, `DOUBLE PRECISION`, `FLOAT`), NUMERIC (`NUMERIC`, `DECIMAL(10,5)`,
      `BOOLEAN`, `DATE`, `DATETIME`). Affinity is what decides whether `'123'` lands as text or as
      an integer, so a copy that loses a declared type changes the *values* a tenant reads back.
    * **Column-level schema features** — `NOT NULL`, `DEFAULT`, `CHECK`, `UNIQUE`, `COLLATE`
      (`NOCASE`/`RTRIM`), foreign keys, `INTEGER PRIMARY KEY AUTOINCREMENT`, composite primary
      keys, `WITHOUT ROWID`, `STRICT`, and generated columns (both `STORED` and `VIRTUAL`).
    * **Schema objects around the fields** — plain, unique, partial, and expression indexes, a
      view, and a trigger.

  ## Determinism

  The fuzz is seeded (`:seed`, default `#{20_260_731}`), so the same options always produce a
  byte-identical database. A benchmark whose fixture moved between runs would measure the
  fixture; a test whose fixture moved could not fail the same way twice. `build!/2` saves and
  restores the caller's `:rand` state, so seeding here never perturbs the calling process.

  Edge values are **not** left to the fuzz. `@edge_rows` pins the cases that actually break
  things — i64 min/max, `-0.0`, subnormal and huge floats, the empty string against NULL, the
  empty blob, a blob full of NUL bytes, multi-byte and RTL text — so they are present at any row
  count. The random rows exist to add volume and variety on top of that floor.

  Two SQLite behaviors are deliberately *not* fuzzed, because they are lossy by design and would
  make a round-trip assertion wrong rather than strict:

    * **NaN is stored as NULL.** SQLite has no NaN in its type system, so a REAL NaN comes back
      `nil`. Inserting one would assert that a value survives when nothing preserved it.
    * **NUMERIC affinity converts.** `'123'` into a `NUMERIC` column is stored as the integer
      `123`. That conversion is correct and is pinned explicitly in the edge rows rather than
      hidden inside random data.

  ## Usage

      Fathom.Keystone.build!(path)                       # 256 rows, default seed
      Fathom.Keystone.build!(path, rows: 100_000)        # benchmark volume
      Fathom.Keystone.build!(path, rows: 512, seed: 7)   # a different, still-reproducible fuzz

  Lives in `lib/` rather than `test/support/` on purpose: `mix fathom.bench` runs under
  `MIX_ENV=prod`, where `test/support` is not compiled (see `elixirc_paths/1` in `mix.exs`), and
  the whole point is that the benchmark and the suite measure the *same* keystone.
  """

  alias Fathom.Shard.Connection

  @default_rows 256
  @default_seed 20_260_731

  # Every table the keystone defines, in dependency order (parents before children). Callers that
  # diff two keystones walk this list, so a new table is covered by the round-trip test the moment
  # it is added here.
  @tables ~w(ks_scalars ks_constraints ks_generated ks_strict ks_without_rowid ks_child)

  @doc "The keystone's tables, in dependency order (parents first)."
  @spec tables() :: [String.t()]
  def tables, do: @tables

  @doc "Default number of fuzzed rows per table when `:rows` is not given."
  @spec default_rows() :: pos_integer()
  def default_rows, do: @default_rows

  # --- schema ----------------------------------------------------------------

  @doc """
  The keystone's DDL, as a list of SQL strings in application order.

  Returned separately from `build!/2` so a caller can replay the schema through the migration
  engine (see `statement_pairs/0`) instead of only creating it directly.
  """
  @spec schema_statements() :: [String.t()]
  def schema_statements do
    [
      # --- every affinity, every declared-type spelling, all nullable so NULL appears in each ---
      #
      # The column list follows SQLite's affinity determination rules in order: a declared type
      # containing "INT" is INTEGER; then CHAR/CLOB/TEXT is TEXT; then BLOB or no type is BLOB;
      # then REAL/FLOA/DOUB is REAL; everything else is NUMERIC. Each branch is spelled more than
      # one way, because it is the *substring match* that assigns affinity, and a copy that
      # normalizes declared types would change how values are stored.
      """
      CREATE TABLE ks_scalars (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        c_int INT,
        c_integer INTEGER,
        c_tinyint TINYINT,
        c_smallint SMALLINT,
        c_mediumint MEDIUMINT,
        c_bigint BIGINT,
        c_ubigint UNSIGNED BIG INT,
        c_int2 INT2,
        c_int8 INT8,
        c_character CHARACTER(20),
        c_varchar VARCHAR(255),
        c_varying VARYING CHARACTER(255),
        c_nchar NCHAR(55),
        c_native NATIVE CHARACTER(70),
        c_nvarchar NVARCHAR(100),
        c_text TEXT,
        c_clob CLOB,
        c_blob BLOB,
        c_no_type,
        c_real REAL,
        c_double DOUBLE,
        c_double_precision DOUBLE PRECISION,
        c_float FLOAT,
        c_numeric NUMERIC,
        c_decimal DECIMAL(10,5),
        c_boolean BOOLEAN,
        c_date DATE,
        c_datetime DATETIME
      )
      """,

      # --- column constraints and collations ---
      #
      # COLLATE is here because it is stored in the schema, not the value: two rows that compare
      # equal under NOCASE are distinct bytes, so a copy that dropped the collation would keep
      # every byte and still change query results.
      """
      CREATE TABLE ks_constraints (
        id INTEGER PRIMARY KEY,
        required TEXT NOT NULL,
        with_default TEXT NOT NULL DEFAULT 'dflt',
        bounded INTEGER CHECK (bounded BETWEEN 0 AND 100),
        uniq TEXT UNIQUE,
        ci TEXT COLLATE NOCASE,
        rt TEXT COLLATE RTRIM
      )
      """,

      # --- generated columns, STORED and VIRTUAL ---
      #
      # STORED occupies bytes in the file; VIRTUAL is computed at read time. A copy that
      # materialized VIRTUAL (or dropped STORED) would still round-trip every value and be wrong.
      """
      CREATE TABLE ks_generated (
        id INTEGER PRIMARY KEY,
        base INTEGER NOT NULL,
        doubled INTEGER GENERATED ALWAYS AS (base * 2) STORED,
        labeled TEXT GENERATED ALWAYS AS ('v' || base) VIRTUAL
      )
      """,

      # --- STRICT: SQLite enforces the declared type instead of applying affinity ---
      # Only INT, INTEGER, REAL, TEXT, BLOB and ANY are legal here. ANY is the one column in the
      # keystone that can hold all five storage classes in the same column.
      """
      CREATE TABLE ks_strict (
        id INTEGER PRIMARY KEY,
        s_int INTEGER NOT NULL,
        s_real REAL NOT NULL,
        s_text TEXT NOT NULL,
        s_blob BLOB NOT NULL,
        s_any ANY
      ) STRICT
      """,

      # --- WITHOUT ROWID + composite primary key: a different on-disk b-tree layout entirely ---
      """
      CREATE TABLE ks_without_rowid (
        k_text TEXT NOT NULL,
        k_int INTEGER NOT NULL,
        payload BLOB,
        PRIMARY KEY (k_text, k_int)
      ) WITHOUT ROWID
      """,

      # --- foreign key to the rowid table ---
      """
      CREATE TABLE ks_child (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES ks_scalars(id) ON DELETE CASCADE,
        note TEXT
      )
      """,

      # --- indexes: plain, unique, partial, expression ---
      "CREATE INDEX ks_scalars_text_idx ON ks_scalars (c_text)",
      "CREATE UNIQUE INDEX ks_constraints_uniq_idx ON ks_constraints (uniq)",
      "CREATE INDEX ks_scalars_partial_idx ON ks_scalars (c_integer) WHERE c_integer IS NOT NULL",
      "CREATE INDEX ks_scalars_expr_idx ON ks_scalars (lower(c_text))",
      "CREATE INDEX ks_child_parent_idx ON ks_child (parent_id)",

      # --- a view and a trigger: schema objects a copy must carry, not just tables ---
      """
      CREATE VIEW ks_summary AS
        SELECT s.id AS id, s.c_text AS label, count(c.id) AS children
        FROM ks_scalars s LEFT JOIN ks_child c ON c.parent_id = s.id
        GROUP BY s.id
      """,
      """
      CREATE TRIGGER ks_constraints_default_ci
      AFTER INSERT ON ks_constraints
      WHEN NEW.ci IS NULL
      BEGIN
        UPDATE ks_constraints SET ci = 'auto' WHERE id = NEW.id;
      END
      """
    ]
  end

  @doc """
  `schema_statements/0` as `{sql, args}` pairs — the shape `Fathom.Migrator.Copy` binds, so the
  keystone's schema can be replayed through the real migration path.
  """
  @spec statement_pairs() :: [{String.t(), list()}]
  def statement_pairs, do: Enum.map(schema_statements(), &{&1, []})

  # --- build -----------------------------------------------------------------

  @doc """
  Creates the keystone database at `path` and returns `{:ok, row_count}`.

  Options:

    * `:rows` — fuzzed rows per table (default `#{@default_rows}`). The pinned edge rows are
      always present, so a count below their number still yields a valid keystone; it simply
      contains no random rows.
    * `:seed` — fuzz seed (default `#{@default_seed}`). The same seed always builds the same file.
    * `:journal_mode` — defaults to `WAL`, matching how fathom opens a shard.
  """
  @spec build!(String.t(), keyword()) :: {:ok, non_neg_integer()}
  def build!(path, opts \\ []) do
    rows = Keyword.get(opts, :rows, @default_rows)
    seed = Keyword.get(opts, :seed, @default_seed)
    journal = Keyword.get(opts, :journal_mode, "WAL")

    {:ok, conn} = Connection.open(path)

    # Seeding :rand is process-global. Save the caller's state and put it back, so a test that
    # builds a keystone does not silently re-seed whatever else that process does afterward.
    prev = :rand.export_seed()

    try do
      :rand.seed(:exsss, {seed, seed * 7 + 1, seed * 13 + 3})

      :ok = Connection.exec(conn, "PRAGMA journal_mode=#{journal}")
      :ok = Connection.exec(conn, "PRAGMA foreign_keys=ON")

      for ddl <- schema_statements(), do: :ok = Connection.exec(conn, ddl)

      :ok = Connection.exec(conn, "BEGIN")
      count = seed_rows(conn, rows)
      :ok = Connection.exec(conn, "COMMIT")

      :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
      {:ok, count}
    after
      # `export_seed/0` returns the atom `:undefined` in a process that has never used :rand —
      # and `:undefined` is TRUTHY in Elixir, so a bare `if prev` fed it straight to
      # `:rand.seed/1`, which died in `:rand.mk_alg/1`. Invisible to the suite, because ExUnit
      # has already seeded every test process; the benchmark runs in a fresh one and hit it on
      # the first try.
      if prev != :undefined, do: :rand.seed(prev)
      Connection.close(conn)
    end
  end

  # --- rows ------------------------------------------------------------------

  # The values that actually break a copy, pinned so they are present at every row count. Each
  # entry is the `ks_scalars` payload after `id`; nil means SQL NULL.
  #
  # Ordering matters only in that the first row is all-NULL: a column that is never NULL cannot
  # prove that NULL survives, and NULL is the storage class most easily lost in a rewrite.
  @edge_rows [
    # 1. every nullable column NULL.
    :all_null,
    # 2. 64-bit integer boundaries — the values that overflow a 32-bit or float64 round-trip.
    {:ints, [-9_223_372_036_854_775_808, 9_223_372_036_854_775_807, 0, -1, 1]},
    # 3. text edges: empty string (distinct from NULL), quote, backslash, newline, tab.
    {:text, ["", "it's", "back\\slash", "line1\nline2", "tab\there"]},
    # 4. multi-byte, RTL, emoji, combining marks — anything that a byte-length assumption breaks.
    {:text, ["日本語テキスト", "مرحبا بالعالم", "🔑🗝️", "é vs é", String.duplicate("ß", 64)]},
    # 5. blob edges: empty blob, NUL bytes, high bytes, a blob that is valid UTF-8, a long one.
    #    `"text-as-blob"` is the interesting one: identical bytes to a TEXT value, different
    #    storage class, and only the `{:blob, _}` tag keeps them apart.
    {:blobs,
     [
       {:blob, <<>>},
       {:blob, <<0, 0, 0, 0>>},
       {:blob, <<255, 254, 253>>},
       {:blob, "text-as-blob"},
       {:blob, :binary.copy(<<0xAB>>, 512)}
     ]},
    # 6. float edges: negative zero, subnormal, huge, and a value that is not exactly representable.
    {:reals, [-0.0, 5.0e-324, 1.7976931348623157e308, 0.1 + 0.2, -1.5]},
    # 7. NUMERIC affinity conversion, pinned rather than left to chance: '123' is stored as the
    #    INTEGER 123, while '12.5' becomes a REAL and 'abc' stays TEXT.
    {:numeric_text, ["123", "12.5", "abc"]}
  ]

  defp seed_rows(conn, rows) do
    scalar_ids = seed_scalars(conn, rows)
    seed_constraints(conn, rows)
    seed_generated(conn, rows)
    seed_strict(conn, rows)
    seed_without_rowid(conn, rows)
    seed_child(conn, rows, scalar_ids)
    length(scalar_ids)
  end

  @scalar_cols ~w(c_int c_integer c_tinyint c_smallint c_mediumint c_bigint c_ubigint c_int2
                  c_int8 c_character c_varchar c_varying c_nchar c_native c_nvarchar c_text
                  c_clob c_blob c_no_type c_real c_double c_double_precision c_float c_numeric
                  c_decimal c_boolean c_date c_datetime)

  defp seed_scalars(conn, rows) do
    sql =
      "INSERT INTO ks_scalars (" <>
        Enum.join(@scalar_cols, ", ") <>
        ") VALUES (" <> Enum.map_join(@scalar_cols, ", ", fn _ -> "?" end) <> ")"

    edge = Enum.flat_map(@edge_rows, &edge_scalar_rows/1)
    random = for _ <- 1..max(rows - length(edge), 0)//1, do: random_scalar_row()

    all = edge ++ random
    for args <- all, do: {:ok, _} = Connection.query(conn, sql, args)
    Enum.to_list(1..length(all)//1)
  end

  # Every column NULL. `id` is INTEGER PRIMARY KEY so SQLite fills it from the rowid.
  defp edge_scalar_rows(:all_null), do: [Enum.map(@scalar_cols, fn _ -> nil end)]

  # The remaining edge entries drive one affinity family and leave the rest randomized, so each
  # pinned case lands in a row that is otherwise realistic rather than in an all-else-NULL row.
  #
  # A family can have FEWER columns than it has pinned values — there are five pinned blobs but
  # only two BLOB-affinity columns — so one row cannot hold them all. Emit as many rows as it
  # takes and walk the values across them, which makes coverage of every pinned value a property
  # of the generator rather than a coincidence. The earlier version assigned by `phash2(column)`,
  # under which three of the five blobs never appeared at all.
  defp edge_scalar_rows({kind, values}) do
    cols = family_cols(kind)
    width = length(cols)
    rounds = ceil(length(values) / width)

    for round <- 0..(rounds - 1)//1 do
      base = random_scalar_row()

      Enum.zip(@scalar_cols, base)
      |> Enum.map(fn {col, default} ->
        case Enum.find_index(cols, &(&1 == col)) do
          nil -> default
          i -> Enum.at(values, rem(round * width + i, length(values)))
        end
      end)
    end
  end

  defp family_cols(:ints), do: Enum.filter(@scalar_cols, &int_col?/1)
  defp family_cols(:text), do: Enum.filter(@scalar_cols, &text_col?/1)
  defp family_cols(:blobs), do: Enum.filter(@scalar_cols, &blob_col?/1)
  defp family_cols(:reals), do: Enum.filter(@scalar_cols, &real_col?/1)
  defp family_cols(:numeric_text), do: Enum.filter(@scalar_cols, &numeric_col?/1)

  @doc """
  The `ks_scalars` columns belonging to each affinity family, and the pinned edge values the
  generator guarantees are present in each.

  Exposed so the fidelity test can assert coverage against the generator's own definition rather
  than restating a column list that would drift.
  """
  @spec edge_values() :: [{atom(), [String.t()], list()}]
  def edge_values do
    for {kind, values} <- @edge_rows, kind != :all_null, do: {kind, family_cols(kind), values}
  end

  defp int_col?(col),
    do:
      col in ~w(c_int c_integer c_tinyint c_smallint c_mediumint c_bigint c_ubigint c_int2 c_int8)

  defp text_col?(col),
    do: col in ~w(c_character c_varchar c_varying c_nchar c_native c_nvarchar c_text c_clob)

  # `c_no_type` has no declared type at all, which is the second way SQLite assigns BLOB affinity.
  defp blob_col?(col), do: col in ~w(c_blob c_no_type)

  defp real_col?(col), do: col in ~w(c_real c_double c_double_precision c_float)

  defp numeric_col?(col), do: col in ~w(c_numeric c_decimal c_boolean c_date c_datetime)

  defp random_scalar_row do
    Enum.map(@scalar_cols, fn col ->
      cond do
        # ~1 in 8 values is NULL, so NULL appears throughout the file rather than only in the
        # pinned row — a page-level rewrite bug does not respect which row it corrupts.
        :rand.uniform(8) == 1 -> nil
        int_col?(col) -> rand_int()
        text_col?(col) -> rand_text()
        blob_col?(col) -> rand_blob()
        real_col?(col) -> rand_real()
        numeric_col?(col) -> rand_numeric(col)
      end
    end)
  end

  defp rand_int, do: :rand.uniform(4_294_967_296) - 2_147_483_648

  defp rand_real, do: (:rand.uniform() - 0.5) * :math.pow(10, :rand.uniform(12) - 6)

  defp rand_text do
    alphabet = ~w(alpha bravo charlie delta echo 日本 مرحبا 🔑 ß é)
    len = :rand.uniform(6)

    Enum.map_join(1..len//1, " ", fn _ ->
      Enum.at(alphabet, :rand.uniform(length(alphabet)) - 1)
    end)
  end

  # Tagged `{:blob, _}` because exqlite binds a BARE Elixir binary as **TEXT**. Untagged, these
  # bytes land in the BLOB-affinity columns as the TEXT storage class (BLOB affinity applies no
  # conversion, so nothing corrects it), and a STRICT BLOB column rejects them outright with
  # "cannot store TEXT value in BLOB column". The tag is the only thing separating a blob from a
  # text value with identical bytes.
  defp rand_blob do
    len = :rand.uniform(64)
    {:blob, :binary.list_to_bin(for _ <- 1..len//1, do: :rand.uniform(256) - 1)}
  end

  # NUMERIC-affinity columns get values shaped like what an ORM actually writes into them:
  # booleans as 0/1, dates and datetimes as ISO-8601 text, decimals as fixed-point numbers.
  defp rand_numeric("c_boolean"), do: :rand.uniform(2) - 1

  defp rand_numeric("c_date") do
    "20#{pad(:rand.uniform(30))}-#{pad(:rand.uniform(12))}-#{pad(:rand.uniform(28))}"
  end

  defp rand_numeric("c_datetime") do
    rand_numeric("c_date") <>
      " #{pad(:rand.uniform(24) - 1)}:#{pad(:rand.uniform(60) - 1)}:#{pad(:rand.uniform(60) - 1)}"
  end

  defp rand_numeric(_), do: (:rand.uniform(10_000_000) - 5_000_000) / 100

  defp pad(n), do: String.pad_leading(Integer.to_string(n), 2, "0")

  defp seed_constraints(conn, rows) do
    sql =
      "INSERT INTO ks_constraints (id, required, with_default, bounded, uniq, ci, rt) " <>
        "VALUES (?, ?, ?, ?, ?, ?, ?)"

    for i <- 1..rows//1 do
      # `with_default` is left NULL on some rows so the DEFAULT actually fires; `ci` likewise, so
      # the AFTER INSERT trigger fires and the copy has trigger-written rows to carry.
      with_default = if rem(i, 3) == 0, do: "dflt", else: "explicit_#{i}"
      ci = if rem(i, 5) == 0, do: nil, else: "MixedCase#{i}"
      # RTRIM collation only means something if trailing spaces exist.
      rt = "trailing#{i}" <> String.duplicate(" ", rem(i, 4))

      {:ok, _} =
        Connection.query(conn, sql, [
          i,
          "required_#{i}",
          with_default,
          rem(i, 101),
          "uniq_#{i}",
          ci,
          rt
        ])
    end
  end

  defp seed_generated(conn, rows) do
    sql = "INSERT INTO ks_generated (id, base) VALUES (?, ?)"
    for i <- 1..rows//1, do: {:ok, _} = Connection.query(conn, sql, [i, rand_int()])
  end

  defp seed_strict(conn, rows) do
    sql =
      "INSERT INTO ks_strict (id, s_int, s_real, s_text, s_blob, s_any) VALUES (?, ?, ?, ?, ?, ?)"

    for i <- 1..rows//1 do
      # s_any rotates through all five storage classes in one column — only possible under STRICT
      # with an ANY column, and the tightest single check that class is preserved.
      any =
        case rem(i, 5) do
          0 -> nil
          1 -> rand_int()
          2 -> rand_real()
          3 -> rand_text()
          4 -> rand_blob()
        end

      {:ok, _} =
        Connection.query(conn, sql, [i, rand_int(), rand_real(), rand_text(), rand_blob(), any])
    end
  end

  defp seed_without_rowid(conn, rows) do
    sql = "INSERT INTO ks_without_rowid (k_text, k_int, payload) VALUES (?, ?, ?)"

    for i <- 1..rows//1 do
      {:ok, _} = Connection.query(conn, sql, ["key_#{rem(i, 16)}", i, rand_blob()])
    end
  end

  defp seed_child(conn, rows, scalar_ids) do
    sql = "INSERT INTO ks_child (id, parent_id, note) VALUES (?, ?, ?)"
    parents = length(scalar_ids)

    for i <- 1..rows//1 do
      # Some children are orphans (NULL parent) so the FK column carries NULL too.
      parent = if rem(i, 7) == 0, do: nil, else: Enum.at(scalar_ids, rem(i, parents))
      {:ok, _} = Connection.query(conn, sql, [i, parent, "note_#{i}"])
    end
  end

  # --- fidelity ---------------------------------------------------------------

  @doc """
  A comparable dump of the whole keystone: every table's rows **and** each value's SQLite
  `typeof()`, keyed by table name.

  Comparing values alone is not enough. Affinity decides the storage class a value lands in, so a
  copy that dropped a declared type could return `"123"` where the source returns `123` — equal
  under a loose comparison, a different type to every client. Comparing `typeof()` alongside the
  value catches that.
  """
  @spec dump(String.t()) :: %{String.t() => %{values: list(), types: list()}}
  def dump(path) do
    {:ok, conn} = Connection.open(path)

    try do
      Map.new(@tables, fn table ->
        {:ok, %{columns: columns}} = Connection.query(conn, "SELECT * FROM #{table} LIMIT 0", [])

        order = order_by(table)
        {:ok, values} = Connection.query(conn, "SELECT * FROM #{table} ORDER BY #{order}", [])

        types_select = Enum.map_join(columns, ", ", fn c -> "typeof(\"#{c}\")" end)

        {:ok, types} =
          Connection.query(conn, "SELECT #{types_select} FROM #{table} ORDER BY #{order}", [])

        {table, %{values: values.rows, types: types.rows}}
      end)
    after
      Connection.close(conn)
    end
  end

  # WITHOUT ROWID has no rowid to order by, so each table names its own stable key. An unordered
  # comparison would be a false pass: two files can hold the same rows in a different b-tree order.
  defp order_by("ks_without_rowid"), do: "k_text, k_int"
  defp order_by(_), do: "id"
end
