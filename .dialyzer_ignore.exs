# Dialyzer warnings suppressed on purpose.
#
# EMPTY IS THE GOAL. Every entry is a claim, and each carries a comment saying WHY it is here and
# WHAT would let it go. An entry that only says "noisy" is a bug being hidden.
#
# `list_unused_filters: true` in mix.exs fails the run on a filter that no longer matches, so this
# file cannot quietly accumulate entries that stopped being true. That is also what keeps the
# line-pinned entries below honest — they WILL go stale as these files move, and the run says so
# instead of silently suppressing the wrong thing.
#
# Granularity, and its cost: `{file, warning, line}` does NOT match warnings that carry a column
# (`pattern_match_cov`, `guard_fail`, `no_return`, `call` all do), so those entries are the
# coarser `{file, warning}` form. That suppresses the whole warning TYPE in that file, so a FUTURE
# unintended one there would be hidden — a real cost, accepted only where the file's instances are
# all deliberate and named in the comment above the entry. Prefer the three-element form whenever
# dialyzer will honour it.
#
# There are exactly two legitimate reasons to be in this file, and the difference matters:
#
#   (1) DIALYZER IS WRONG. Its analysis cannot represent something true about the code — a
#       dependency's opaque type, a polymorphic helper, an unenforced struct field.
#
#   (2) DIALYZER IS RIGHT ABOUT TODAY, AND THE CLAUSE IS DELIBERATE ANYWAY. An unreachable
#       fail-safe kept because the thing that makes it unreachable is not guaranteed — a
#       dependency version, a documented "fails safe to", a cardinality bound.
#
# Neither is "fix it later". Fix real findings in code. Two shapes that LOOK like false positives
# and are not:
#
#   * a function that always raises or exits — spec it `no_return()` (see `mix fathom.token`)
#   * a spec narrower than the success typing — the spec is wrong, or the code is; pick one
#
# Scope note: the gate runs `MIX_ENV=dev mix dialyzer`, which analyzes `lib/` only — see the
# comment on `dialyzer/0` in mix.exs for why `test/support` is out and what that costs.
#
# Format: {file} | {file, warning} | {file, warning, line} | a regex on the message.
[
  # --- (1) dialyzer is wrong -------------------------------------------------------------------

  # `Fathom.Bench.with_wire/3` is a higher-order helper called by four metrics whose callbacks
  # return different shapes — a `%{p50_us, p99_us}` map from `hrana_rt_stats/1`, a bare float from
  # the other three. Dialyzer computes ONE success typing for the helper that unions all four, then
  # hands that union to every caller, so each is reported as missing (or having an extra) shape it
  # never returns. Each of these specs is accurate for its own function.
  #
  # A polymorphic `@spec ... when result: var` on the helper does NOT fix it and is not worth
  # retrying: dialyzer uses SUCCESS TYPINGS, not contracts, when analyzing callers. Measured twice
  # on 2026-08-14 (also on `HranaClient.await_upgrade/2`), and written up next to the helper.
  # Removable if these metrics stop sharing one helper, or if dialyzer gains per-call-site
  # instantiation.
  {"lib/fathom/bench.ex", :missing_range, 668},
  {"lib/fathom/bench.ex", :missing_range, 731},
  {"lib/fathom/bench.ex", :missing_range, 823},
  {"lib/fathom/bench.ex", :extra_range, 881},

  # `Quorum.remaining/1` is `max(0, q - MapSet.size(acked))` and `next_version/0` is an increment
  # over a Postgres aggregate; both are declared as integers and dialyzer allows `float()`, because
  # struct field types and Ecto aggregate results are not enforced at runtime and it cannot rule a
  # float out. `Quorum.new/2` guards `is_integer/1` on both fields, so every properly-constructed
  # struct satisfies the spec. Widening the specs to `number()` would document a value the code
  # deliberately refuses to construct.
  {"lib/fathom/shard/replication/quorum.ex", :missing_range, 114},
  {"lib/fathom/migrator.ex", :missing_range, 557},

  # `RateLimiter.bump/4` calls `:ets.update_counter/3` with a single `{pos, incr}` op, which returns
  # an integer; the list-of-ops form returns a list. OTP's spec is the union of both, and dialyzer
  # cannot narrow on the literal tuple. Removable if OTP's spec gains overloads.
  {"lib/fathom/rate_limiter.ex", :missing_range, 45},

  # Attributed to line 1, i.e. macro-expanded rather than fathom source: `Fathom.Shard` is
  # `use GenServer, restart: :temporary` with an overridden `child_spec/1` that calls `super()`.
  # There is no line-1 code to fix, and no conditional in the module's own source that matches
  # (`grep` for compile-time branches finds none). Removable if a future Elixir/OTP attributes it
  # to real source, at which point it should be re-read rather than re-ignored.
  {"lib/fathom/shard.ex", :pattern_match, 1},

  # --- (2) deliberate, and unreachable only for reasons that are not guaranteed -----------------

  # Two deliberate fallbacks in one file, so one filter each covers both:
  #
  #   * `changes/1` and `last_rowid/1` accept BOTH `{:ok, n}` and a bare integer, because exqlite
  #     has returned each across versions. Dialyzer only ever sees the ONE version installed today,
  #     so it calls the other clause dead — which is exactly the case the clause exists for.
  #     Deleting it would turn a dependency bump into a FunctionClauseError on the write path.
  #   * `autocommit?/1`'s `_ -> true`. Its own @doc says "Fails safe to `true` (the historical
  #     default) if the status can't be read", so the fallback IS the documented contract, and
  #     `Sqlite3.transaction_status/1` returning something else is a dependency question.
  #
  # Removable if fathom ever pins exqlite hard enough to make both shapes guaranteed.
  {"lib/fathom/shard/connection.ex", :pattern_match_cov},
  {"lib/fathom/shard/connection.ex", :guard_fail},

  # REMOVED 2026-08-21 (expert review #35). This covered `Shards.fork_failure_reason/1`'s
  # `_ -> :unknown`, which dialyzer called unreachable because every caller of `report_fork/2` was
  # inside this module and the outcome shapes were therefore statically known. `report_fork/2` is
  # now `@doc false` PUBLIC so its classification can be tested directly, so its input is no longer
  # knowable and the catch-all is genuinely reachable. `list_unused_filters: true` caught the stale
  # entry, which is the whole reason that option is on.

  # `Shard.flush_position/1`'s no-lease clause. It is guarded `when is_integer(epoch)` on a
  # destructured `%{lease: %{epoch: epoch}}`, and every state dialyzer can see has one — but the
  # clause answers the no-lease case with `nil`, which the comment above it explains is the SAFE
  # answer: an absent stamp reads as "unknown" and makes the object un-overridable. Crashing there
  # instead would fail a durability flush.
  {"lib/fathom/shard.ex", :pattern_match_cov},

  # `Bench.seed_dead_lock/1` passes a NEGATIVE ttl to `Storage.acquire_lease/3`, whose contract is
  # `pos_integer()`. This is deliberate and commented: it seeds an already-expired lock so the next
  # open STEALS it, which is how the S3 failover metrics exercise the crash-failover path.
  #
  # The contract is RIGHT for production and the caller is abusing it, so this is not a spec fix.
  # It is also probably vestigial: `expires_at_ms` is no longer the liveness signal (the node
  # heartbeat is — see the moduledoc on `Fathom.Shard.Storage`), and `seed_dead_lock/1`'s own
  # comment says the absent heartbeat is what resolves the owner dead. Resolving it means running
  # the failover benches with a positive ttl, which needs `FATHOM_S3_TEST_*` — so it is recorded
  # here rather than changed blind. The three `no_return`s are this same call cascading.
  {"lib/fathom/bench.ex", :call},
  {"lib/fathom/bench.ex", :no_return}
]
