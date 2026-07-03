defmodule Fathom.ShardId do
  @moduledoc """
  The single validator for shard ids — the shard-isolation and path-traversal gate.

  A shard id becomes a SQLite file name, a `Fathom.ShardRegistry` key, an S3 object
  key, and (via a pre-validated interpolation) part of a `VACUUM INTO` string literal
  in `Fathom.Shard`. So it is constrained to a conservative character set and length.

  This module is the **one** source of that rule. Routing (request → shard, via
  `Fathom.ShardExecutor.shard_from_conn/1`) and find-or-start (`Fathom.Shards.ensure/1`)
  both call `valid?/1`, so the isolation contract can never desync between the request
  path and the file path — the failure mode a duplicated regex invites. See the
  shard-isolation gate in AGENTS.md.
  """

  # Alphanumerics, underscore, hyphen; 1..64 chars. No dot (blocks `..` traversal and
  # dotted-subdomain ambiguity), no slash, no whitespace, no control chars.
  @pattern ~r/^[a-zA-Z0-9_-]{1,64}$/

  @doc """
  Whether `id` is a valid shard id (the isolation gate). Non-binaries are invalid, so
  callers can pass untrusted request-derived values straight in.
  """
  @spec valid?(term()) :: boolean()
  def valid?(id) when is_binary(id), do: id =~ @pattern
  def valid?(_), do: false

  @doc """
  Validates AND canonicalizes `id`: `{:ok, downcased_id}` for a valid id, `:error` otherwise.

  Case is **normalized to lower** (finding #19) so `ACME` and `acme` name the *same* shard —
  otherwise they are distinct on a case-sensitive filesystem (a silently split tenant) yet collide
  on a case-insensitive one, and an LB that lowercases `Host` for hashing disagrees with fathom's
  naming. Downcasing is ASCII-only (`valid?` admits only `[a-zA-Z0-9_-]`, so `:ascii` is
  locale-independent and can't surprise), and validity is invariant under it, so the result is
  always itself `valid?`. Call this at the trust boundaries (request → shard, find-or-start) so
  every downstream use — registry key, file path, S3 key, directory row, coordinator handle — sees
  the one canonical value.
  """
  @spec cast(term()) :: {:ok, String.t()} | :error
  def cast(id) when is_binary(id) do
    if valid?(id), do: {:ok, String.downcase(id, :ascii)}, else: :error
  end

  def cast(_), do: :error
end
