defmodule Fathom.FailureCaptureFormatter do
  @moduledoc """
  An ExUnit formatter that writes every test failure to a timestamped file, with the seed and a
  ready-to-paste rerun command.

  ## Why this exists

  Two intermittent single-test failures (2026-07-25, during expert-review #26 and #36) lost their
  identity permanently. Both were seen as a bare count — "965/966 passed" — because the run's
  output was piped through `tail`, and by the time anyone looked, the next run had overwritten
  ExUnit's `--failed` manifest. Roughly 30 subsequent full-suite runs and 25 seed-swept runs never
  reproduced either, so the information was simply gone.

  A flake you cannot name is a flake you cannot fix. This makes naming it automatic instead of
  dependent on someone remembering not to truncate the output.

  ## What it writes

  Nothing at all on a clean run — the file is created lazily on the first failure, so a green suite
  leaves no litter. On failure, `logs/test-failures-<timestamp>.log` gets the seed, the failing
  test's module/name/location, the formatted failure with its stacktrace, and the exact command to
  replay it. The seed matters most: `--seed` fixes test ORDER, and an order-dependent flake only
  reproduces deterministically once you have it.

  Registered alongside `ExUnit.CLIFormatter` in `test/test_helper.exs`, so normal console output is
  unchanged.
  """
  use GenServer

  @dir "logs"

  @doc false
  def init(opts) do
    {:ok,
     %{
       seed: opts[:seed],
       path: nil,
       count: 0,
       # `--trace`/`--max-failures` etc. change the rerun command; keep what we were given.
       width: opts[:width] || 80
     }}
  end

  @doc false
  def handle_cast({:suite_started, opts}, state) do
    {:noreply, %{state | seed: opts[:seed] || state.seed}}
  end

  def handle_cast({:test_finished, %ExUnit.Test{state: {:failed, failures}} = test}, state) do
    state = ensure_file(state)
    count = state.count + 1

    body =
      ExUnit.Formatter.format_test_failure(
        test,
        failures,
        count,
        state.width,
        # No ANSI colour in a file.
        fn _kind, msg -> msg end
      )

    File.write!(
      state.path,
      [
        "\n",
        String.duplicate("=", 78),
        "\n",
        # inspect/1, not interpolation: the latter renders the raw atom as "Elixir.Foo.BarTest".
        "#{inspect(test.module)}\n",
        "  #{test.name}\n",
        "  #{test.tags.file}:#{test.tags.line}\n",
        "  rerun: mix test --seed #{state.seed} #{relative(test.tags.file)}:#{test.tags.line}\n",
        String.duplicate("=", 78),
        "\n",
        body,
        "\n"
      ],
      [:append]
    )

    {:noreply, %{state | count: count}}
  end

  # An invalid test (its setup_all failed) never runs, so it has no failure to format — but the
  # module that broke is worth recording, since a setup_all crash takes a whole file with it.
  def handle_cast({:test_finished, %ExUnit.Test{state: {:invalid, module}} = test}, state) do
    state = ensure_file(state)

    File.write!(
      state.path,
      "\nINVALID (setup_all failed): #{inspect(module)} — #{test.module}.#{test.name}\n",
      [:append]
    )

    {:noreply, state}
  end

  def handle_cast({:suite_finished, _}, %{path: nil} = state), do: {:noreply, state}

  def handle_cast({:suite_finished, _}, state) do
    # Point at the file from the console too, so a truncated view still surfaces the path.
    IO.puts("\n#{state.count} failure(s) captured to #{state.path}")
    {:noreply, state}
  end

  def handle_cast(_event, state), do: {:noreply, state}

  # Created on the FIRST failure, never before: a green run must leave no file behind.
  defp ensure_file(%{path: nil} = state) do
    File.mkdir_p!(@dir)
    stamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%d-%H%M%S")
    path = Path.join(@dir, "test-failures-#{stamp}.log")

    File.write!(path, """
    fathom test failures — #{DateTime.utc_now() |> DateTime.to_string()}
    seed: #{state.seed}

    Replay this exact run (order included):  mix test --seed #{state.seed}
    Rerun only what failed:                  mix test --failed
    NOTE: `mix test --failed` reads a manifest the NEXT run overwrites. Use it before re-running.
    """)

    %{state | path: path}
  end

  defp ensure_file(state), do: state

  defp relative(file), do: Path.relative_to(file, File.cwd!())
end
