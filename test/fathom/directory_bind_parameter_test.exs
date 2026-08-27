defmodule Fathom.DirectoryBindParameterTest do
  @moduledoc """
  Pins a MEASUREMENT that a review got wrong, so the wrong version cannot come back.

  Expert review 2026-08-26 #21 claimed `Directory.requeue_failed/1` would fail past ~65 000 ids
  because "Ecto expands `in ^ids` to one bind parameter per element" and Postgres caps a statement
  at 65 535 parameters. It does not: `field in ^list` compiles to `field = ANY($1)` — **one**
  parameter carrying an array.

  The finding conflated two mechanisms that look alike and are not:

    * `Fathom.Migrator.enqueue_unique/1` really does chunk at 5 000, because Oban's `insert_all`
      emits one parameter per column per ROW. Its comment records a real crash past ~7 281 jobs,
      found by `scripts/directory_scale.exs` at 3.1M rows.
    * a `WHERE … IN` does not grow that way at all.

  So the chunking that finding asked for would have added a loop and a comment describing a hazard
  that does not exist. What was real — `failed_shards/0` materializing the whole quarantined slice
  as structs — is fixed by `Directory.stream_failed/1`.

  This test exists because the claim is about a DEPENDENCY's behaviour, not fathom's. If a future
  Ecto or Postgrex changed the expansion, `requeue_failed/1` would silently become the hazard the
  review described. This makes that change loud, and its failure message says what to do.
  """
  use Fathom.DataCase, async: true

  import Ecto.Query

  alias Fathom.Directory.Shard

  @big 70_000

  test "an `in ^ids` list is ONE array bind parameter, not one per element" do
    ids = Enum.map(1..@big, &"s#{&1}")

    {sql, params} =
      Ecto.Adapters.SQL.to_sql(
        :all,
        Fathom.Repo,
        from(s in Shard, where: s.status == "migration_failed" and s.shard_id in ^ids)
      )

    assert length(params) == 1,
           """
           `in ^ids` now expands to #{length(params)} bind parameters instead of one array.

           Postgres caps a statement at 65 535 parameters, so `Directory.requeue_failed/1` is now
           genuinely unbounded and MUST be chunked (5 000, matching `Migrator.@enqueue_chunk`).
           Read the comment on `requeue_failed/1` first — it records why chunking was measured and
           removed, and that reasoning has just stopped holding.
           """

    assert [list] = params
    assert is_list(list) and length(list) == @big

    assert sql =~ "= ANY(",
           "the array form is what makes this one parameter; see the failure message above"
  end

  test "the same holds for the update_all this is actually about" do
    ids = Enum.map(1..@big, &"s#{&1}")

    {_sql, params} =
      Ecto.Adapters.SQL.to_sql(
        :update_all,
        Fathom.Repo,
        from(s in Shard,
          where: s.status == "migration_failed" and s.shard_id in ^ids,
          update: [set: [status: "active"]]
        )
      )

    # The `set:` contributes its own parameters, so this is not `== 1` — the invariant is that the
    # count does not scale with the id list.
    assert length(params) < 10,
           "update_all now emits #{length(params)} parameters for #{@big} ids — it scales with " <>
             "the list, so `Directory.requeue_failed/1` must be chunked"
  end
end
