defmodule Mix.Tasks.Fathom.Bench.Check do
  @moduledoc """
  Compare a just-run working-tree bench against the parent commit's baseline and
  exit with a gate verdict. Used by `scripts/commit_with_bench.sh`; not usually run
  by hand.

      mix fathom.bench.check --parent <sha> [options]

  ## Why two history files

  The pre-commit bench records under the **current HEAD** (= the parent — HEAD
  hasn't moved yet), so the new line and the parent's baseline line share a SHA.
  To keep them apart, `commit_with_bench.sh` snapshots `perf_history.jsonl` BEFORE
  benching and passes it as `--parent-history`; the parent baseline is the last
  same-SHA, same-host, same-env line in the snapshot, and the new run is the last
  line of the live `--new-history`.

  ## Options

    * `--parent <sha>`           — parent commit to compare against (required)
    * `--parent-history PATH`    — history snapshot taken before benching
                                   (default scripts/perf_history.jsonl)
    * `--new-history PATH`       — live history with the new run appended
                                   (default scripts/perf_history.jsonl)
    * `--block N` / `--warn N`   — regression percent thresholds (default 20 / 20)
    * `--host H`                 — compare same-host only (default: the new run's host)

  ## Exit codes

    * `0` OK · `2` no parent baseline (can't gate) · `3` WARN band · `4` BLOCK
  """
  @shortdoc "Compare a working-tree bench to the parent baseline; exit with a gate verdict"

  use Mix.Task

  alias Fathom.Bench.Gate

  @switches [
    parent: :string,
    parent_history: :string,
    new_history: :string,
    block: :integer,
    warn: :integer,
    host: :string
  ]

  @default_history "scripts/perf_history.jsonl"

  @impl true
  def run(argv) do
    {opts, _, _} = OptionParser.parse(argv, strict: @switches)
    parent_sha = Keyword.fetch!(opts, :parent)
    parent_history = Keyword.get(opts, :parent_history, @default_history)
    new_history = Keyword.get(opts, :new_history, @default_history)
    block = Keyword.get(opts, :block, 20)
    warn = Keyword.get(opts, :warn, 20)

    new = new_history |> read_lines() |> List.last()

    if is_nil(new) do
      Mix.shell().error("bench gate: no run found in #{new_history} — nothing to compare.")
      System.halt(2)
    end

    host = Keyword.get(opts, :host) || new["host"]
    # Compare same-env too: a dev run (~3x slower, file I/O bound) seeded into the
    # history must never serve as the baseline for a prod gate, or it reads as a
    # huge false regression. The gate is always run prod via benchmark.sh.
    env = new["mix_env"]
    parent = parent_line(parent_history, parent_sha, host, env)

    if is_nil(parent) do
      Mix.shell().error(
        "bench gate: no baseline for parent #{short(parent_sha)} " <>
          "(host #{host}, env #{env}) in #{parent_history}.\n  → Bench the parent first, " <>
          "then retry, or commit with --skip / [skip-bench] if this is genuinely the " <>
          "first bench."
      )

      System.halt(2)
    end

    result = Gate.compare(parent, new, block, warn)
    Mix.shell().info(Gate.format(result, short(parent_sha), block, warn))

    case result.verdict do
      v when v in [:ok, :no_data] -> :ok
      :warn -> System.halt(3)
      :block -> System.halt(4)
    end
  end

  # The parent baseline: the most recent same-SHA, same-host, same-env line.
  defp parent_line(path, sha, host, env) do
    path
    |> read_lines()
    |> Enum.filter(fn m ->
      commit_match?(m, sha) and m["host"] == host and m["mix_env"] == env
    end)
    |> List.last()
  end

  defp commit_match?(m, sha) do
    full = m["commit_full"] || ""
    short = m["commit"] || ""
    short == sha or String.starts_with?(full, sha) or String.starts_with?(sha, short)
  end

  defp read_lines(path) do
    case File.read(path) do
      {:ok, body} ->
        body
        |> String.split("\n", trim: true)
        |> Enum.map(&Jason.decode/1)
        |> Enum.flat_map(fn
          {:ok, map} -> [map]
          _ -> []
        end)

      {:error, _} ->
        []
    end
  end

  defp short(sha), do: String.slice(sha, 0, 7)
end
