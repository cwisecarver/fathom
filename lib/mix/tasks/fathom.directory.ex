defmodule Mix.Tasks.Fathom.Directory do
  @shortdoc "Directory / control-plane operator tools (cross-store DR reconcile)"
  @moduledoc """
  Directory operator tooling.

      mix fathom.directory reconcile [--fix] [--limit N]

  **reconcile** — the cross-store DR sweep (expert review #6). Fathom's correctness state spans the
  Postgres directory and object storage; a Postgres point-in-time restore can desync them
  (resurrected deletes, un-revoked tokens, rewound `schema_version`). This realigns the directory to
  the authoritative durable facts in storage and is the **DR-completion step: run it with `--fix`
  after a Postgres restore, before reopening traffic.** Read-only by default (a dry run); `--fix`
  applies corrections. `--limit N` bounds how many rows it inspects (each pulls the shard's object).

  Reconciles: `schema_version` ← the file's `PRAGMA user_version`; `token_version` ← the durable
  storage revocation floor; and re-tombstones any `deleted`-in-storage id the directory forgot.
  """
  use Mix.Task

  alias Fathom.Directory.Reconcile

  @requirements ["app.start"]

  @impl Mix.Task
  def run(argv) do
    {opts, rest, _} =
      OptionParser.parse(argv, strict: [fix: :boolean, limit: :integer])

    case rest do
      ["reconcile"] -> reconcile(opts)
      _ -> usage()
    end
  end

  defp reconcile(opts) do
    fix? = Keyword.get(opts, :fix, false)
    findings = Reconcile.run(fix: fix?, limit: opts[:limit])

    if findings == [] do
      Mix.shell().info("reconcile: directory and storage are coherent — nothing to fix.")
    else
      Mix.shell().info(
        "reconcile: #{length(findings)} finding(s)#{if fix?, do: " (fixing)", else: " (dry run — pass --fix to apply)"}:"
      )

      Enum.each(findings, &print_finding/1)
      summarize(findings, fix?)
    end
  end

  defp print_finding(%{kind: :schema_drift} = f),
    do:
      Mix.shell().info(
        "  #{f.shard_id}: schema_version #{f.from} -> #{f.to} (file user_version)#{fixed(f)}"
      )

  defp print_finding(%{kind: :token_drift} = f),
    do:
      Mix.shell().info(
        "  #{f.shard_id}: token_version #{f.from} -> #{f.to} (storage floor)#{fixed(f)}"
      )

  defp print_finding(%{kind: :orphan_tombstone} = f),
    do:
      Mix.shell().info(
        "  #{f.shard_id}: storage tombstone with no deleted row -> re-tombstone#{fixed(f)}"
      )

  defp print_finding(%{kind: :missing_object} = f),
    do: Mix.shell().error("  #{f.shard_id}: directory row has NO stored object (dangling row)")

  defp fixed(%{fixed: true}), do: " [FIXED]"
  defp fixed(_), do: ""

  defp summarize(findings, fix?) do
    by_kind = Enum.frequencies_by(findings, & &1.kind)
    Mix.shell().info("summary: #{inspect(by_kind)}")

    unless fix? do
      Mix.shell().info(
        "Re-run with --fix to apply (after a Postgres restore, before reopening traffic)."
      )
    end
  end

  defp usage do
    Mix.shell().error("usage: mix fathom.directory reconcile [--fix] [--limit N]")
  end
end
