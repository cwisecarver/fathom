defmodule Fathom.Shard.FlushInput do
  @moduledoc """
  What the snapshot-and-upload chain reads off the coordinator, as a struct.

  The periodic flush task used to receive a hand-picked `Map.take` of the coordinator state while
  the drop path handed the chain the WHOLE state, so every function in the chain read its inputs
  defensively (`Map.get(state, :put_intents, [])`, `Map.get(state, :coordinator)`, a
  `recheck_before_put/1` fallback clause) and an omission from the `Map.take` list failed late and
  quietly: the `carry_meta` failure mode, a hand-picked key list that loses an entry (cleanliness
  audit 2026-11-13 #28). Both paths now build this struct with `from_state/2`, so the field set is
  declared once and `@enforce_keys` makes a missing one a compile/build error at the call site
  instead of a silent default inside a flush task.

  Field notes (what each is for, since the chain is long):

    * `:id`, `:path`, `:etag`: the shard, its file, and the etag the object PUT is conditional on.
    * `:lease`: what the 412-reconcile re-checks. The fence returns a refreshed one; `with_fence/2`
      merges it.
    * `:acquire_gen`: `nil` in legacy mode. `recheck_before_put/1` asks the heartbeat fence only when
      it is an integer. The fence also returns it (`Fence.updates`), merged the same way.
    * `:lineage`, `:carried_lineage`: the stamp's lineage and the one carried across a fork.
      `:lineage` is read STRICTLY downstream (`lineage_to_store/1`): there is deliberately no default.
    * `:wal_salt`, `:wal_ordinal`: the position stamp. `Position.flush_position/3` stamps an ordinal
      only when the salt it reads off the WAL MATCHES this one, so a nil can only WITHHOLD an ordinal
      (the pre-A2 answer: the stored object wins), never invent one.
    * `:touch_pre_etag`: the etag the lease-touch left, so a flush that lands on it re-touches.
    * `:put_intents`: each attempt's md5, for the 412-reconcile's ownership question
      (`Fork.put_origin/3`). `[]` when the coordinator holds none.
    * `:coordinator`: the pid the flush task reports each attempt's md5 to from its `:pre_put` hook
      (the `{:put_intent, _}` messages the 412-reconcile reads). `nil` on the drop path, whose upload
      runs in a `Task.async` of its own and has no consumer for those messages.

  Not the fence context (`fence_ctx/1`: lease/ttl/acquire_gen for `Fence.check/1`), which stays a
  separate, smaller map.
  """

  alias Fathom.Shard.Storage

  @enforce_keys [:id, :path, :etag, :lease, :lineage, :acquire_gen]
  defstruct [
    :id,
    :path,
    :etag,
    :lease,
    :lineage,
    :acquire_gen,
    :wal_salt,
    :wal_ordinal,
    :carried_lineage,
    :touch_pre_etag,
    :coordinator,
    put_intents: []
  ]

  @type t :: %__MODULE__{
          id: Fathom.ShardId.t(),
          path: Path.t(),
          etag: String.t() | nil,
          lease: Storage.lease(),
          lineage: non_neg_integer() | :unknown | :disabled,
          acquire_gen: non_neg_integer() | nil,
          wal_salt: integer() | nil,
          wal_ordinal: non_neg_integer(),
          carried_lineage: non_neg_integer() | :none | nil,
          touch_pre_etag: String.t() | nil,
          coordinator: pid() | nil,
          put_intents: [String.t()]
        }

  @doc """
  Build the chain's input from the coordinator state. `coordinator` is the pid the flush task reports
  to (`self()` for the periodic flush, `nil` on the drop path).

  `:acquire_gen` is read from the state here, BEFORE the fence runs; on the periodic path
  `with_fence/2` then overwrites it (and the lease) with the fence's verdict, which is the value
  `recheck_before_put/1` must see. Callers of the periodic path must therefore apply `with_fence/2`
  on every `{:ok, updates}`.

  The required fields are read STRICTLY (`state.lineage`, not `Map.get`): they exist in every
  coordinator state, and a state that lacks one is a bug to surface here, in the coordinator, rather
  than a default inside a flush task.
  """
  @spec from_state(map(), pid() | nil) :: t()
  def from_state(state, coordinator \\ nil) do
    %__MODULE__{
      id: state.id,
      path: state.path,
      etag: state.etag,
      lease: state.lease,
      lineage: state.lineage,
      acquire_gen: state.acquire_gen,
      wal_salt: state.wal_salt,
      wal_ordinal: state.wal_ordinal,
      carried_lineage: state.carried_lineage,
      touch_pre_etag: state.touch_pre_etag,
      put_intents: Map.get(state, :put_intents, []),
      coordinator: coordinator
    }
  end

  @doc """
  Merge the fence's verdict back in (`Fence.updates`: `%{lease:, acquire_gen:}`). `struct!/2`, so a
  fence that ever returned another key fails here instead of growing a stray key on the input.
  """
  @spec with_fence(t(), %{lease: Storage.lease(), acquire_gen: non_neg_integer() | nil}) :: t()
  def with_fence(%__MODULE__{} = input, updates), do: struct!(input, updates)
end
