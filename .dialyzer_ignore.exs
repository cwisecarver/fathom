# Dialyzer warnings suppressed on purpose.
#
# EMPTY IS THE GOAL. Every entry is a claim that dialyzer is wrong and fathom is right, so each one
# carries a comment saying WHY it is a false positive and WHAT would make it removable. An entry
# that only says "noisy" is a bug being hidden.
#
# `list_unused_filters: true` in mix.exs makes a stale filter fail the run, so this file cannot
# quietly accumulate entries that stopped matching.
#
# Fix real findings in code. Two shapes that look like false positives but are not:
#   * a function that always raises or exits — spec it `no_return()`, do not ignore it
#   * a spec narrower than the success typing — the spec is wrong, or the code is; pick one
#
# Filo is a Hex dependency, so its beams are in the PLT and its typings are known, but only
# fathom's CALL SITES are analyzed — a filo-internal problem never surfaces here. A wrong `@spec`
# inside filo CAN false-positive at a fathom call site; such an entry belongs here and must name
# the filo function, with an upstream issue. Filo's own dialyzer setup is out of scope.
#
# Format: {file} | {file, warning} | {file, warning, line} | a regex on the message.
[]
