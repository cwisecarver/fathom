defmodule Fathom.Shard.Replication.Protocol do
  @moduledoc """
  The A2 replication wire format — see `docs/a2-quorum-replication.md`.

  One message shape in each direction, carried inside `gen_tcp`'s `packet: 4` length framing so
  this module never has to find message boundaries itself.

  ## Why explicit binary rather than `term_to_binary`

  `:erlang.binary_to_term/1` on bytes from a socket is an atom-exhaustion and code-execution
  surface even between our own nodes, and `binary_to_term(_, [:safe])` still admits shapes we did
  not intend. AGENTS.md's "never `String.to_atom` on outside input" is the same rule one layer
  down. The wire is a fixed-width header plus an opaque payload; nothing here allocates an atom
  from received bytes.

  ## The three fields that make this safe

  A follower is appending bytes into a SQLite WAL. Get the ordering wrong and it does not error —
  it silently produces a corrupt database. So every push carries the state needed to *refuse*:

    * `epoch` — the shard's lease epoch (`Fathom.Shard.Storage`'s existing fencing token). A
      follower must never accept frames from a primary that has already been superseded, or a
      deposed node's writes land on top of the new owner's.
    * `wal_gen` — bumped whenever the primary's WAL restarts (a checkpoint truncates it and SQLite
      rewrites the header with fresh salts). Byte offset 5000 before a checkpoint and byte offset
      5000 after are unrelated positions, so the follower must be told to discard and start over
      rather than appending across the seam.
    * `offset` — where in the current WAL generation this payload begins. The follower rejects
      anything that is not exactly where its own copy ends, which turns a lost or reordered push
      into a retryable error instead of a corrupt file.

  Gate 1 proved the follower side works by appending byte ranges at the right offsets
  (`test/fathom/shard/wal_apply_test.exs`); these three fields are what make that safe when the
  ranges arrive over a network that can drop, reorder and duplicate.
  """

  @version 1

  @push 1
  @ack 2
  @reject 3

  # Reject reasons. Integers on the wire — see the moduledoc on not minting atoms from bytes.
  @reasons %{
    1 => :offset_mismatch,
    2 => :stale_epoch,
    3 => :stale_wal_gen,
    4 => :unknown_shard,
    5 => :internal
  }
  @reason_codes Map.new(@reasons, fn {k, v} -> {v, k} end)

  defmodule Push do
    @moduledoc "A primary's frame delta for one shard."
    @enforce_keys [:shard_id, :epoch, :wal_gen, :offset, :payload]
    defstruct [:shard_id, :epoch, :wal_gen, :offset, :payload]

    @type t :: %__MODULE__{
            shard_id: String.t(),
            epoch: non_neg_integer(),
            wal_gen: non_neg_integer(),
            offset: non_neg_integer(),
            payload: binary()
          }
  end

  @doc """
  Encode a frame delta. Returns iodata — the payload is not copied.
  """
  @spec encode_push(Push.t()) :: iodata()
  def encode_push(%Push{} = p) do
    shard = p.shard_id

    [
      <<@version::8, @push::8, byte_size(shard)::16, p.epoch::64, p.wal_gen::64, p.offset::64>>,
      shard,
      p.payload
    ]
  end

  @doc """
  Acknowledge durably holding everything up to (not including) `next_offset`.

  The ack carries the follower's *resulting* offset rather than a bare "ok" so the primary can
  detect divergence without a separate round trip: an ack for an offset the primary did not expect
  means the two sides disagree about the follower's state, which is the same class of bug the
  `offset` field exists to prevent.
  """
  @spec encode_ack(non_neg_integer()) :: iodata()
  def encode_ack(next_offset), do: <<@version::8, @ack::8, next_offset::64>>

  @doc """
  Refuse a push, and say where the follower actually is.

  `expected_offset` lets the primary rewind and re-send rather than tear the follower down and
  re-seed it from S3 — a full re-seed is the expensive recovery, and a gap is the cheap one.
  """
  @spec encode_reject(atom(), non_neg_integer()) :: iodata()
  def encode_reject(reason, expected_offset) when is_map_key(@reason_codes, reason) do
    <<@version::8, @reject::8, @reason_codes[reason]::8, expected_offset::64>>
  end

  @doc """
  Decode any message. Returns `{:error, :malformed}` rather than raising: bytes arriving on a
  socket are the one input that is never under our control, and a crash here would take down a
  connection the supervisor would then rebuild into the same crash.
  """
  @spec decode(binary()) ::
          {:ok, Push.t()}
          | {:ok, {:ack, non_neg_integer()}}
          | {:ok, {:reject, atom(), non_neg_integer()}}
          | {:error, :malformed | :unsupported_version}
  def decode(<<@version::8, @push::8, slen::16, epoch::64, gen::64, off::64, rest::binary>>)
      when byte_size(rest) >= slen do
    <<shard::binary-size(^slen), payload::binary>> = rest

    {:ok, %Push{shard_id: shard, epoch: epoch, wal_gen: gen, offset: off, payload: payload}}
  end

  def decode(<<@version::8, @ack::8, next::64>>), do: {:ok, {:ack, next}}

  def decode(<<@version::8, @reject::8, code::8, expected::64>>)
      when is_map_key(@reasons, code) do
    {:ok, {:reject, @reasons[code], expected}}
  end

  # A version mismatch is worth distinguishing from garbage: it is the signal for a rolling
  # deploy that has gone further than the protocol allows, and it wants a different operator
  # response (finish or roll back the deploy) than a corrupt frame does.
  def decode(<<v::8, _::binary>>) when v != @version, do: {:error, :unsupported_version}
  def decode(_), do: {:error, :malformed}

  @doc "The protocol version this node speaks."
  @spec version() :: pos_integer()
  def version, do: @version
end
