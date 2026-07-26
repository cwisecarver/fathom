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

  defp release_images do
    Enum.filter(@dockerfiles, fn path -> File.read!(path) =~ ~r/^RUN\s+mix\s+release/m end)
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

      assert body =~ ~r/^COPY\s+fathom\/rel\s+rel\s*$/m,
             "#{path} runs `mix release` without copying fathom/rel — the release will silently " <>
               "generate a default vm.args and drop every flag in rel/vm.args.eex"

      copy_at = index_of(body, ~r/^COPY\s+fathom\/rel\s+rel\s*$/m)
      release_at = index_of(body, ~r/^RUN\s+mix\s+release/m)

      assert copy_at < release_at,
             "#{path} copies fathom/rel AFTER `mix release`, so the release is built without it"
    end
  end

  test "every deny-all dockerignore for a release image whitelists fathom/rel" do
    for path <- release_images() do
      ignore = path <> ".dockerignore"

      if File.exists?(ignore) do
        body = File.read!(ignore)

        # Only a deny-all whitelist can drop rel/; an allow-by-default ignore file cannot.
        if body =~ ~r/^\*\s*$/m do
          assert body =~ ~r/^!fathom\/rel\s*$/m,
                 "#{ignore} denies everything by default but does not whitelist fathom/rel, so " <>
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
