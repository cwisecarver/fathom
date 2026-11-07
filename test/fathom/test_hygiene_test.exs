defmodule Fathom.TestHygieneTest do
  @moduledoc """
  Expert review 2026-11-07 #87: five `async: true` modules mutated node-global state (Application
  env), so any unrelated async test that ran meanwhile could observe the changed value (a
  `fork_from_template` flag, a widened tenant-pragma allow-list). Every one restored it in
  `on_exit`, so the effect was a WINDOW, not a leak, and only an occasional flake showed it.

  ExUnit runs every `async: true` module concurrently and only then the `async: false` ones
  serially, so a module that sets global env must be `async: false`. This test keeps that from
  regressing: it fails when a module declaring `async: true` calls a function that writes
  Application or OS environment. A test that needs the setting AND concurrency has to scope it
  another way (an option on the function under test, a per-process override), not widen this list.
  """
  use ExUnit.Case, async: true

  # Built from parts so this file does not match its own scan.
  @writers [
    ~r/Application\.(put|delete)_env/,
    ~r/System\.(put|delete)_env/,
    ~r/Env\.put!/
  ]

  @spec async_modules() :: [String.t()]
  defp async_modules do
    "test/**/*.exs"
    |> Path.wildcard()
    |> Enum.reject(&(&1 == __ENV__.file |> Path.relative_to_cwd()))
    |> Enum.filter(fn path ->
      File.read!(path) =~ ~r/^\s*use [\w.]+,[^\n]*async:\s*true/m
    end)
  end

  test "no async: true module writes Application or OS environment" do
    offenders =
      for path <- async_modules(),
          source = File.read!(path),
          writer <- @writers,
          Regex.match?(writer, source),
          do: {path, Regex.source(writer)}

    assert offenders == [],
           "async: true modules that mutate node-global env (make them async: false): " <>
             inspect(offenders)
  end

  test "the scan sees the async modules it is meant to police" do
    # A glob that silently matched nothing would make the test above pass forever.
    assert length(async_modules()) > 10
  end
end
