#!/bin/sh
# Apply the carried exqlite patch (BLOB cells tagged as {:blob, bin}; expert review 2026-11-04 #1)
# to deps/exqlite and rebuild its NIF from source. Idempotent. Needs a C toolchain.
# Usage: scripts/apply_exqlite_patch.sh   (run from the repo root; set MIX_ENV as needed)
set -eu
cd "$(dirname "$0")/.."
patch_file="$PWD/native/exqlite_patch/0001-multi-step-tagged-blob.patch"

if grep -q multi_step_tagged deps/exqlite/c_src/sqlite3_nif.c; then
  echo "exqlite patch already applied"
else
  (cd deps/exqlite && patch -p1 <"$patch_file")
fi

FATHOM_EXQLITE_PATCHED=1 mix deps.compile exqlite --force
