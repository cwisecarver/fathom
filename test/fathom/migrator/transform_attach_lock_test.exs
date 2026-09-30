defmodule Fathom.Migrator.TransformAttachLockTest do
  @moduledoc """
  `attach_transform/2` and the migrator's chain read cannot interleave (expert review 2026-09-29
  #12, the row-lock tier the first #12 fix left open).

  The first fix made the migrator mark a shard `migrating` BEFORE reading its replay chain, and
  `attach_transform/2` refuses while any shard is `migrating` below the version. Both are
  check-then-act, so they could still cross: the attach counted before the mark committed, and the
  chain was read before the transform committed — the shard replays without the transform while
  every later shard gets it, a split fleet with all three version stamps agreeing.

  The fix is two Postgres row locks: the attach takes FOR UPDATE on the release row BEFORE its
  counts, and the chain read takes FOR SHARE. Each test below holds one side's lock by hand and
  shows the other side waits for it and then sees what it committed.

  ## Why this file leaves the SQL sandbox

  A row lock is a property of two CONNECTIONS. Under the sandbox every process shares one
  connection and one transaction, so nothing can ever block and both tests would pass against the
  unfixed code. So each process checks out a real connection (`sandbox: false`), the rows are
  COMMITTED, and the file is `async: false` — ExUnit runs sync modules alone, so no concurrent test
  can see a release at these versions — and deletes what it wrote on exit.
  """
  use ExUnit.Case, async: false

  import Ecto.Query

  alias Fathom.Directory.Shard
  alias Fathom.Migrator
  alias Fathom.Migrator.Release
  alias Fathom.Repo

  defmodule Backfill do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(_conn, _shard_id), do: :ok
  end

  setup do
    real_conn!()
    prev = Application.get_env(:fathom, :migration_transforms)
    Application.put_env(:fathom, :migration_transforms, [Backfill])

    # Far above anything another test creates, and unique per run.
    version = 900_000 + System.unique_integer([:positive])
    shard_id = "attachlock#{System.unique_integer([:positive])}"

    Repo.insert!(%Release{version: version, name: "attach_lock", statements: []})

    on_exit(fn ->
      real_conn!()
      Repo.delete_all(from(r in Release, where: r.version == ^version))
      Repo.delete_all(from(s in Shard, where: s.shard_id == ^shard_id))

      if prev,
        do: Application.put_env(:fathom, :migration_transforms, prev),
        else: Application.delete_env(:fathom, :migration_transforms)

      try do
        Fathom.Migrator.HeadCache.refresh()
      catch
        :exit, _ -> :ok
      end
    end)

    %{version: version, shard_id: shard_id}
  end

  defp real_conn!, do: :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo, sandbox: false)

  # A process that opens a transaction, runs `inside` (which takes a lock), reports `:locked`, and
  # commits only when told to — so the test controls exactly how long the lock is held.
  defp hold_lock(inside) do
    test = self()

    pid =
      spawn_link(fn ->
        real_conn!()

        Repo.transaction(fn ->
          inside.()
          send(test, :locked)

          receive do
            :commit -> :ok
          end
        end)

        send(test, :committed)
      end)

    assert_receive :locked, 5_000
    pid
  end

  test "the chain read waits for an in-flight attach and then carries its transform", ctx do
    %{version: v} = ctx

    # The attach's side: the release row locked FOR UPDATE, the transform about to be written.
    holder =
      hold_lock(fn ->
        Repo.one!(from(r in Release, where: r.version == ^v, lock: "FOR UPDATE"))

        Repo.update_all(from(r in Release, where: r.version == ^v),
          set: [transform: to_string(Backfill)]
        )
      end)

    reader =
      Task.async(fn ->
        real_conn!()
        Migrator.statement_steps([v])
      end)

    assert Task.yield(reader, 300) == nil,
           "the chain read did not wait for the attach holding the release row — it read the " <>
             "version WITHOUT the transform, which is the split fleet #12 is about"

    send(holder, :commit)
    assert_receive :committed, 5_000

    assert %{^v => {_statements, transform}} = Task.await(reader, 5_000)
    assert transform == to_string(Backfill), "the chain read did not re-read the committed row"
  end

  test "an attach waits for an in-flight mark + chain read, then counts the migrating shard",
       ctx do
    %{version: v, shard_id: shard_id} = ctx

    # The migrator's side, compressed into one transaction so the lock spans it: the shard marked
    # `migrating` below `v` and the release row read FOR SHARE. In production the mark commits first
    # and the FOR SHARE read follows; holding both here is the worst case for the attach — its count
    # must still come after them.
    holder =
      hold_lock(fn ->
        Repo.insert!(%Shard{
          shard_id: shard_id,
          schema_version: v - 1,
          status: "migrating",
          last_active_at: DateTime.utc_now()
        })

        Repo.all(from(r in Release, where: r.version == ^v, lock: "FOR SHARE"))
      end)

    attach =
      Task.async(fn ->
        real_conn!()
        Migrator.attach_transform(v, Backfill)
      end)

    assert Task.yield(attach, 300) == nil,
           "attach_transform did not wait for the chain read holding the release row — it counted " <>
             "migrating shards before that shard's mark was visible and would accept the transform"

    send(holder, :commit)
    assert_receive :committed, 5_000

    assert {:error, {:migration_in_flight, 1}} = Task.await(attach, 5_000)

    refute Repo.one!(from(r in Release, where: r.version == ^v, select: r.transform)),
           "the refused attach still wrote the transform"
  end
end
