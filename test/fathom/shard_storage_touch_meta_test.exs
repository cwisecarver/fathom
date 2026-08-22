defmodule Fathom.ShardStorageTouchMetaTest do
  @moduledoc """
  What a steal-time touch must carry forward — `Fathom.Shard.Storage.S3.carry_meta/1`.

  ## The bug this pins

  A takeover "touches" the shard object with a server-side self-copy to rotate its etag, which is
  what fences the deposed node's `If-Match` flush. S3 requires the `REPLACE` metadata directive for
  a self-copy, and **REPLACE drops every user metadata key unless it is sent again**.

  The touch re-sent one key: the integrity md5. `x-amz-meta-fathom-pos` — the position stamp A2
  compares a replica against — was added later and was never added here, so **every takeover
  silently erased it**. An unstamped object is never overridable by design, so promote-on-open and
  survivor selection both went inert at exactly the moment they exist for. Nothing failed and
  nothing logged; the shard just recovered to its last flush, which is the pre-A2 behaviour.

  Measured on the chaos rig 2026-08-12 (`chaos.sh rpo`): the stamp read
  `%{epoch: 1, wal_gen: 0, offset: 0}` before the kill and `nil` after, while a peer held a replica
  at offset 8272 that could have been promoted.

  ## Why this test is a list assertion rather than a behavioural one

  The behaviour — S3 dropping metadata on a REPLACE copy — is a property of the real store.
  `Storage.Local` and `Fathom.Test.FaultyStorage` both keep their metadata map in place across a
  touch, so **no test written against either double can reproduce this**, and one written against
  them would pass before and after the fix. That is the environment gap AGENTS.md describes, and it
  is why the fix is "carry every key" rather than "carry this second key": the failure mode is an
  omission from a list, so the list itself is the thing worth pinning.

  **The list survived until 2026-08-22.** The moduledoc above described the right fix and the code
  still enumerated `[@md5_meta, @pos_meta]`; it was found while adding a third key, which would
  have been the third chance at the identical omission. `carry_meta/1` now carries every
  `x-amz-meta-` header, and the first test below is the one that pins the class rather than the
  instance.

  The live-store counterpart lives in `shard_storage_s3_test.exs` (tagged `:s3`), which can assert
  the stamp survives an actual touch.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Storage.S3

  @md5 "x-amz-meta-fathom-md5"
  @pos "x-amz-meta-fathom-pos"

  # THE CLASS, not the instance (2026-08-22). `carry_meta/1` was still a hand-picked list
  # `[@md5_meta, @pos_meta]` — this file's own moduledoc already said the fix is "carry every key
  # rather than carry this second key", and the code had not caught up. Found while adding the
  # lineage key for #8, which would have been the third chance to make the identical omission.
  #
  # This is the assertion that closes it: a key nothing in `carry_meta/1` has ever heard of is
  # carried anyway. It fails against any list-based implementation, however long the list.
  test "a key the function has never heard of is carried too" do
    assert {"x-amz-meta-fathom-invented-tomorrow", "42"} in S3.carry_meta(%{
             "x-amz-meta-fathom-invented-tomorrow" => "42"
           }),
           "carry_meta/1 is enumerating a fixed list again. The failure mode is an OMISSION from " <>
             "that list, and it has already cost this project one silent months-long outage of " <>
             "promote-on-open and survivor selection."
  end

  test "non-user headers are not carried — this is user metadata, not a header copy" do
    carried = S3.carry_meta(%{"content-type" => "application/octet-stream", @md5 => "abc"})
    assert carried == [{@md5, "abc"}]
  end

  test "header names are matched case-insensitively, as RFC 9110 requires" do
    assert {"x-amz-meta-fathom-pos", "7:2:8272"} in S3.carry_meta(%{
             "X-Amz-Meta-Fathom-Pos" => "7:2:8272"
           })
  end

  test "the position stamp is carried across a touch" do
    # THE REGRESSION. Deleting `@pos_meta` from `carry_meta/1`'s key list fails only this
    # assertion, which is the whole point — the md5 one below kept passing throughout the bug.
    carried = S3.carry_meta(%{@md5 => "abc123", @pos => "7:2:8272"})

    assert {@pos, "7:2:8272"} in carried
    assert {@md5, "abc123"} in carried
  end

  test "an absent key is skipped, never fabricated" do
    # A legacy object with no md5 must not gain one, and an object with no position must stay
    # unstamped rather than acquire a stamp claiming an ordering nobody established — that would
    # invent exactly the comparison `Promote.fresher?/2` refuses to guess at.
    assert S3.carry_meta(%{@md5 => "abc123"}) == [{@md5, "abc123"}]
    assert S3.carry_meta(%{@pos => "7:2:8272"}) == [{@pos, "7:2:8272"}]
    assert S3.carry_meta(%{}) == []
  end

  test "list-valued headers are read, since that is the shape Req hands back" do
    # Req represents repeated headers as lists, and a nil here would silently drop the key —
    # the same class of failure as omitting it from the list in the first place.
    carried = S3.carry_meta(%{@md5 => ["abc123"], @pos => ["7:2:8272"]})

    assert {@pos, "7:2:8272"} in carried
    assert {@md5, "abc123"} in carried
  end

  test "unrelated metadata is not carried" do
    # The sentinel marker must NOT ride along: a touch that carried it would turn a real object
    # into something the pull path reads as a placeholder.
    carried = S3.carry_meta(%{@md5 => "abc123", "x-amz-meta-fathom-sentinel" => "1"})

    assert carried == [{@md5, "abc123"}]
  end
end
