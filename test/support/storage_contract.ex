defmodule Fathom.Test.StorageContract do
  @moduledoc """
  One shared suite asserting every subtle `Fathom.Shard.Storage` callback contract, run against
  **both** backends (expert review 2026-08-01 #30, item 8).

  ## Why this exists

  #30's whole shape was "the suite pins the happy paths and has no discriminating test for any
  path where the system concludes success on weak evidence." Its sharpest instance was measured:
  the `Local` double and the S3 backend disagreed about what a return value MEANS, so a class of
  bug could only exist in S3 and `mix test` was structurally incapable of seeing it.

  That is not hypothetical twice over:

    * **#9** — `Local` identifies a lock by `{owner, epoch}` while S3 fences with an etag, so the
      first regression test for a stale-lease bug PASSED against the unfixed code.
    * **#24** — S3 returned `{:ok, etag}` from `pull/2` having written no file. Nine consumers
      then opened a path that did not exist, which CREATES an empty database. The chaos rig found
      it; the suite could not, because `Local.pull/2` cannot produce that shape.

  A per-backend test proves one backend does the right thing. This proves they mean the SAME
  thing, which is the property the coordinator actually depends on when `:shard_storage` is
  swapped.

  ## How to use

      defmodule Fathom.Shard.Storage.ContractLocalTest do
        use ExUnit.Case, async: false
        use Fathom.Test.StorageContract, backend: Fathom.Shard.Storage.Local
        setup do: ...configure the store...
      end

  The using module supplies the setup that points the backend at a fresh, empty store. Every
  assertion below goes through the public `Storage` API — nothing pokes at files or at the fake
  S3 agent — so the same code is meaningful for any future backend.

  ## What is deliberately NOT here

  * **Steal sentinels.** They are an S3-only concept and `local.ex:447` documents WHY: content-hash
    etags cannot rotate without the bytes changing, and the backend is single-node, so the zombie
    race the sentinel fences cannot arise. That is a deliberate constraint, not a gap — sentinel
    behaviour is pinned in `s3_sentinel_test` / `s3_sentinel_copy_test` instead.
  * **#44's source-vs-destination etag divergence.** `Local.flush/3` hashes the SOURCE after
    copying, but `file_etag/1` and `content_etag/1` are the same sha256, so with a quiescent
    source the two agree and a contract test cannot tell them apart. Catching #44 needs a
    mutate-the-source-mid-flush seam that neither live backend has. Stated so the next reader does
    not mistake this suite's silence for coverage.
  """

  defmacro __using__(opts) do
    backend = Keyword.fetch!(opts, :backend)

    quote location: :keep do
      @backend unquote(backend)

      defp tmp_file(body) do
        path =
          Path.join(
            System.tmp_dir!(),
            "contract_#{System.unique_integer([:positive])}.db"
          )

        File.write!(path, body)
        on_exit(fn -> File.rm(path) end)
        path
      end

      defp dest_path do
        path =
          Path.join(
            System.tmp_dir!(),
            "contract_dest_#{System.unique_integer([:positive])}.db"
          )

        on_exit(fn -> File.rm(path) end)
        path
      end

      defp sid(base), do: "#{base}_#{System.unique_integer([:positive])}"

      describe "#{inspect(unquote(backend))} — pull/2's written-or-not contract (#24)" do
        test "an absent object returns :absent and writes NOTHING" do
          dest = dest_path()

          assert {:absent, _etag} = @backend.pull(sid("gone"), dest)

          refute File.exists?(dest),
                 "a backend that reports :absent must not leave a file; a later Connection.open/1 " <>
                   "on this path would CREATE an empty database and serve it as the tenant's data"
        end

        test "{:ok, etag} means the bytes ARE at local_path" do
          id = sid("present")
          :ok = @backend.flush(id, tmp_file("shard-bytes"))
          dest = dest_path()

          assert {:ok, etag} = @backend.pull(id, dest)
          assert is_binary(etag)
          assert File.exists?(dest), "{:ok, _} promises bytes at local_path"
          assert File.read!(dest) == "shard-bytes"
        end

        test "pull_snapshot/3 obeys the same contract in both directions" do
          id = sid("snapped")
          missing = dest_path()

          assert {:absent, _} = @backend.pull_snapshot(id, "nope", missing)
          refute File.exists?(missing)

          :ok = @backend.flush(id, tmp_file("snap-bytes"))
          :ok = @backend.snapshot(id, "s1")
          dest = dest_path()

          assert {:ok, _etag} = @backend.pull_snapshot(id, "s1", dest)
          assert File.read!(dest) == "snap-bytes"
        end
      end

      describe "#{inspect(unquote(backend))} — the flush fence" do
        test "object_etag/1 is {:ok, nil} when nothing is stored" do
          assert {:ok, nil} = @backend.object_etag(sid("never"))
        end

        test "a brand-new fenced flush (nil) succeeds once, then is superseded" do
          id = sid("newborn")

          assert {:ok, etag, _} = @backend.flush(id, tmp_file("first"), nil)
          assert is_binary(etag)

          # A second create-only flush must 412: this is what stops a zombie's stalled first
          # flush from landing after a steal.
          assert {:error, :superseded} = @backend.flush(id, tmp_file("zombie"), nil)
          assert stored_body(id) == "first", "the superseded flush must not have written"
        end

        test "the etag flush/3 returns describes the STORED object" do
          # The coordinator fences its NEXT flush with this value, so if it describes anything
          # other than what is now stored, the next flush 412s and the shard self-fences away
          # acknowledged writes.
          id = sid("fence")

          assert {:ok, etag, _} = @backend.flush(id, tmp_file("v1"), nil)
          assert {:ok, ^etag} = @backend.object_etag(id)

          assert {:ok, etag2, _} = @backend.flush(id, tmp_file("v2"), etag)
          assert {:ok, ^etag2} = @backend.object_etag(id)
        end

        test "a stale expected_etag is superseded and leaves the object untouched" do
          id = sid("stale")
          {:ok, etag1, _} = @backend.flush(id, tmp_file("v1"), nil)
          {:ok, _etag2, _} = @backend.flush(id, tmp_file("v2"), etag1)

          assert {:error, :superseded} = @backend.flush(id, tmp_file("v3"), etag1)

          assert stored_body(id) == "v2",
                 "a superseded flush must NOT clobber the newer owner's bytes"
        end
      end

      describe "#{inspect(unquote(backend))} — pull_if_changed/3 (warm-standby freshness)" do
        test "an unchanged object is :unchanged with no byte transfer" do
          id = sid("warm")
          {:ok, etag, _} = @backend.flush(id, tmp_file("body"), nil)
          dest = dest_path()

          assert {:ok, :unchanged} = @backend.pull_if_changed(id, dest, etag)

          refute File.exists?(dest),
                 ":unchanged means the caller's existing copy is current — nothing is written"
        end

        test "a changed object writes fresh bytes and returns the new etag" do
          id = sid("moved")
          {:ok, etag1, _} = @backend.flush(id, tmp_file("old"), nil)
          {:ok, _, _} = @backend.flush(id, tmp_file("new"), etag1)
          dest = dest_path()

          assert {:ok, {:written, _new_etag}} = @backend.pull_if_changed(id, dest, etag1)
          assert File.read!(dest) == "new"
        end

        test "an absent object is :absent, and writes nothing" do
          dest = dest_path()
          assert {:ok, :absent} = @backend.pull_if_changed(sid("nothing"), dest, nil)
          refute File.exists?(dest)
        end
      end

      describe "#{inspect(unquote(backend))} — the lease lifecycle" do
        test "free → acquire → held → release → free" do
          id = sid("lease")

          assert :free = @backend.lease_holder(id)
          assert {:ok, lease} = @backend.acquire_lease(id, "a@node#1", 30_000)
          assert {:held, "a@node#1"} = @backend.lease_holder(id)
          assert :ok = @backend.release_lease(id, lease)
          assert :free = @backend.lease_holder(id)
        end

        test "a live holder refuses a second owner" do
          id = sid("contended")
          {:ok, _} = @backend.acquire_lease(id, "a@node#1", 30_000)

          assert {:error, {:held, "a@node#1", _}} = @backend.acquire_lease(id, "b@node#2", 30_000)
        end

        test "lease_stealable_at agrees with lease_holder, in both directions" do
          # THE invariant that stops the class of drift this callback was added for: a caller
          # PREDICTING a steal must not disagree with the code PERFORMING it. `lease_holder/1`
          # says "stealable now"; `lease_stealable_at/1` says "stealable at T". They are the same
          # question, so held ⇔ T is still in the future — for every backend, by construction.
          id = sid("agree")

          assert :free = @backend.lease_holder(id)
          assert :free = @backend.lease_stealable_at(id)

          {:ok, lease} = @backend.acquire_lease(id, "a@node#1", 30_000)

          assert {:held, "a@node#1"} = @backend.lease_holder(id)
          assert {:held, "a@node#1", at} = @backend.lease_stealable_at(id)

          assert at > System.system_time(:millisecond),
                 "lease_holder reports the shard HELD while lease_stealable_at puts the steal " <>
                   "in the past — a caller would hold for a steal that has already happened, " <>
                   "or refuse one that is available"

          :ok = @backend.release_lease(id, lease)

          assert :free = @backend.lease_holder(id)
          assert :free = @backend.lease_stealable_at(id)
        end

        test "a live owner's stealable instant is at least its lease TTL out" do
          # Sanity on the VALUE, not just its sign: a fresh 30s lease cannot be stealable in the
          # next second, or the crash-hold would wait for every healthy owner.
          id = sid("horizon")
          {:ok, _} = @backend.acquire_lease(id, "a@node#1", 30_000)

          assert {:held, _owner, at} = @backend.lease_stealable_at(id)

          assert at - System.system_time(:millisecond) > 25_000,
                 "a freshly-leased shard reads as stealable far too soon"
        end

        test "check_lease is :ok while held and :superseded once the epoch moves" do
          id = sid("fenced")
          {:ok, lease} = @backend.acquire_lease(id, "a@node#1", 30_000)
          assert :ok = @backend.check_lease(id, lease)

          :ok = @backend.release_lease(id, lease)
          {:ok, _newer} = @backend.acquire_lease(id, "b@node#2", 30_000)

          assert {:error, :superseded} = @backend.check_lease(id, lease)
        end
      end

      describe "#{inspect(unquote(backend))} — the copy primitives" do
        test "fork_shard refuses an absent source and an occupied destination" do
          assert {:error, :no_source} = @backend.fork_shard(sid("nosrc"), sid("dst"))

          src = sid("src")
          dst = sid("dst")
          :ok = @backend.flush(src, tmp_file("src-bytes"))
          :ok = @backend.flush(dst, tmp_file("dst-bytes"))

          assert {:error, :dst_exists} = @backend.fork_shard(src, dst),
                 "never clobber a tenant that already has a stored object"
        end

        test "fork_shard copies the source bytes to a free destination" do
          src = sid("src")
          dst = sid("dst")
          :ok = @backend.flush(src, tmp_file("forked-bytes"))

          assert :ok = @backend.fork_shard(src, dst)
          assert stored_body(dst) == "forked-bytes"
          assert stored_body(src) == "forked-bytes", "the fork is non-disruptive to src"
        end

        test "retain then restore round-trips a version" do
          id = sid("versioned")
          {:ok, etag1, _} = @backend.flush(id, tmp_file("v1"), nil)

          assert :ok = @backend.retain(id, 1)
          {:ok, _, _} = @backend.flush(id, tmp_file("v2"), etag1)
          assert stored_body(id) == "v2"

          assert :ok = @backend.restore(id, 1)
          assert stored_body(id) == "v1"
        end

        test "snapshot then restore_snapshot round-trips" do
          id = sid("snapshotted")
          {:ok, etag1, _} = @backend.flush(id, tmp_file("before"), nil)
          assert :ok = @backend.snapshot(id, "s1")

          {:ok, _, _} = @backend.flush(id, tmp_file("after"), etag1)
          assert :ok = @backend.restore_snapshot(id, "s1")
          assert stored_body(id) == "before"
        end
      end

      describe "#{inspect(unquote(backend))} — purge_shard's id boundary" do
        test "purging `acme` never touches `acme2` (exact id-delimiter match)" do
          # A prefix match here is a cross-tenant data-destruction bug, which is why the
          # delimiter rule is asserted for every backend rather than just the one it was
          # written for.
          base = sid("acme")
          neighbour = base <> "2"

          :ok = @backend.flush(base, tmp_file("mine"))
          :ok = @backend.flush(neighbour, tmp_file("theirs"))
          :ok = @backend.snapshot(neighbour, "keep")

          assert :ok = @backend.purge_shard(base)

          assert {:ok, nil} = @backend.object_etag(base)

          assert stored_body(neighbour) == "theirs",
                 "purging #{base} destroyed #{neighbour} — a prefix match across tenant ids"
        end
      end

      # Read the stored object's bytes through the API alone (pull to a scratch path), so the
      # contract stays backend-agnostic.
      defp stored_body(shard_id) do
        dest = dest_path()

        case @backend.pull(shard_id, dest) do
          {:ok, _etag} -> File.read!(dest)
          {:absent, _} -> nil
          other -> other
        end
      end
    end
  end
end
