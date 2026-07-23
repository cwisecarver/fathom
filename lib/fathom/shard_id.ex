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

  ## DNS / wildcard-TLS caveat

  A shard id is *also* the Host **subdomain** (`<id>.<zone>`). The charset here admits
  the underscore, but an underscore is not a valid DNS-label character, so a **wildcard
  TLS cert** (`*.<zone>`) will not match `some_id.<zone>` — OpenSSL/RFC 6125 refuse to
  expand the wildcard over a label containing `_`. So a tenant meant to be served over
  wildcard TLS must use a **DNS-safe id (letters, digits, hyphens — no underscore)**.
  Underscore ids still work on the plaintext path and would only be reachable over TLS via
  a per-name (non-wildcard) cert. This is a naming guideline, not enforced here — the gate
  stays permissive so the constraint lives with the deployment's TLS choice, not the id
  validator. See the djathom demo, which mints hyphenated ids for exactly this reason.
  """

  # Alphanumerics, underscore, hyphen; 1..64 chars. No dot (blocks `..` traversal and
  # dotted-subdomain ambiguity), no slash, no whitespace, no control chars. NOTE: `_` is
  # admitted but is not a valid DNS label char — see the wildcard-TLS caveat in @moduledoc.
  #
  # Implemented as an allocation-free byte walk, not a regex: this gate runs multiple times
  # per stream open (each trust boundary re-validates — deliberate belt-and-suspenders), and
  # a Regex.match? is ~10× the cost for a rule this small (review 2026-07-23 #21). Byte length
  # is char length here because only ASCII bytes are admitted — any multibyte character fails
  # the byte test itself.

  @doc """
  Whether `id` is a valid shard id (the isolation gate). Non-binaries are invalid, so
  callers can pass untrusted request-derived values straight in.
  """
  @spec valid?(term()) :: boolean()
  def valid?(id) when is_binary(id) and byte_size(id) >= 1 and byte_size(id) <= 64,
    do: valid_bytes?(id)

  def valid?(_), do: false

  defp valid_bytes?(<<c, rest::binary>>)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_ or c == ?-,
       do: valid_bytes?(rest)

  defp valid_bytes?(<<>>), do: true
  defp valid_bytes?(_), do: false

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

  @doc """
  Whether `id` is a valid **DNS label** — i.e. servable under a wildcard TLS cert (`*.<zone>`),
  per the wildcard-TLS caveat above. A `valid?/1` id is DNS-safe iff it *also* satisfies RFC 1035
  label rules the shard-id charset otherwise relaxes: no underscore (RFC 6125 refuses to expand a
  wildcard over a `_` label), ≤ 63 bytes, and no leading/trailing hyphen.

  This is a **superset gate over `valid?/1`, never a replacement** — the isolation validator stays
  permissive (underscore ids still serve on the plaintext path / a per-name cert). It exists so the
  one place that KNOWS the deployment's address — `Fathom.Tenants.provision/1` / `fork/2`, which
  compose the `libsql://<id>.<zone>` URL — can warn or refuse instead of handing back an
  un-servable URL (expert review #35).
  """
  @spec dns_safe?(term()) :: boolean()
  def dns_safe?(id) when is_binary(id) do
    valid?(id) and
      not String.contains?(id, "_") and
      byte_size(id) <= 63 and
      not String.starts_with?(id, "-") and
      not String.ends_with?(id, "-")
  end

  def dns_safe?(_), do: false
end
