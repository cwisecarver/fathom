defmodule Fathom.ReleasePackagingTest do
  # `rel/vm.args.eex` carries every VM capacity flag the node depends on (+SDio, +Q, +IOt/+IOp —
  # expert review 2026-07-24 #2). Neither image copied `rel/` into the build context, and the
  # per-Dockerfile .dockerignore whitelists excluded it, so `mix release` fell back to GENERATING a
  # default vm.args. That fails silently in the worst way: the build succeeds, the release boots,
  # and the flags are simply absent. The chaos rig ran with dirty_io=10 while the release was
  # believed to be running 64 — caught only by reading the limits back out of a booted node.
  #
  # This guard pins the packaging, not the values: any image that runs `mix release` must copy
  # `rel/`, and any deny-all .dockerignore guarding such an image must whitelist it. A new
  # deployment image cannot quietly reintroduce the hole.
  use ExUnit.Case, async: true

  @dockerfiles Path.wildcard("deploy/*/Dockerfile*")
               |> Enum.reject(&String.ends_with?(&1, ".dockerignore"))

  # `RUN mix release`, optionally preceded by BuildKit `--mount=...` options split across `\`
  # continuation lines (the cargo cache mounts, c152bbe) -- a plain `^RUN\s+mix` stopped matching.
  @release_run ~r/^RUN\s+(?:--mount=\S+\s*(?:\\\s*\n\s*)?)*mix\s+release/m

  # `rel/` is `fathom/rel` in an image whose build context is the PARENT of fathom/ (the chaos rig,
  # which can build a sibling filo tree in) and plain `rel` in one whose context is the fathom
  # checkout (the canonical compose image, since cleanliness audit 2026-11-13 #13: filo comes from
  # Hex). The invariant is the same in both layouts, so the guard accepts either spelling.
  @rel_copy ~r/^COPY\s+(?:fathom\/)?rel\s+rel\s*$/m
  @rel_whitelist ~r/^!(?:fathom\/)?rel\s*$/m

  defp release_images do
    Enum.filter(@dockerfiles, fn path -> File.read!(path) =~ @release_run end)
  end

  test "the Dockerfiles that build a release are actually discovered" do
    # If this glob ever stops matching, every assertion below passes vacuously.
    assert release_images() != [],
           "no Dockerfile matching #{inspect(@dockerfiles)} runs `mix release` — " <>
             "the packaging guard below would pass vacuously"
  end

  test "every release image copies rel/ before running mix release" do
    for path <- release_images() do
      body = File.read!(path)

      assert body =~ @rel_copy,
             "#{path} runs `mix release` without copying rel/ — the release will silently " <>
               "generate a default vm.args and drop every flag in rel/vm.args.eex"

      copy_at = index_of(body, @rel_copy)
      release_at = index_of(body, @release_run)

      assert copy_at < release_at,
             "#{path} copies rel/ AFTER `mix release`, so the release is built without it"
    end
  end

  test "every deny-all dockerignore for a release image whitelists rel/" do
    for path <- release_images() do
      ignore = path <> ".dockerignore"

      if File.exists?(ignore) do
        body = File.read!(ignore)

        # Only a deny-all whitelist can drop rel/; an allow-by-default ignore file cannot.
        if body =~ ~r/^\*\s*$/m do
          assert body =~ @rel_whitelist,
                 "#{ignore} denies everything by default but does not whitelist rel/, so " <>
                   "the COPY in #{path} would fail or copy nothing"
        end
      end
    end
  end

  test "rel/vm.args.eex exists and carries the dirty-IO scheduler flag" do
    # The one flag the review called "THE IMPORTANT ONE": every exqlite NIF is dirty-IO bound, so
    # this is the node-wide ceiling on concurrent SQLite work.
    body = File.read!("rel/vm.args.eex")
    assert body =~ ~r/^\+SDio\s+\d+/m, "rel/vm.args.eex no longer sets +SDio"
  end

  defp index_of(body, regex) do
    [{start, _}] = Regex.run(regex, body, return: :index, capture: :first)
    start
  end
end
