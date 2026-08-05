defmodule Mix.Tasks.Compile.FathomUdf do
  @moduledoc """
  Builds `native/fathom_udf` — the SQLite loadable extension supplying Django's UDFs — and installs
  the artifact into `priv/sqlite_ext/` where `Fathom.Shard.Extension` looks for it.

  Wired into `compilers:` in `mix.exs`, so an ordinary `mix compile` keeps it current.

  ## When cargo is missing

  This **does not fail the build**. fathom compiled and ran without this extension for its whole
  life; the consequence of its absence is that Django's UDFs are unavailable and the tracked list
  in `test/fathom/django_udf_compat_test.exs` reports them missing, which is exactly the
  pre-#19 state. Hard-failing would make a Rust toolchain a requirement for every contributor and
  every CI job that has nothing to do with Django compatibility.

  It prints one line saying what it did, because a *silent* skip is how "it works on my machine"
  starts — the failure mode would be a Django app breaking in one deployment and not another, with
  nothing in the build output to explain it.

  ## Environment

    * `FATHOM_SKIP_UDF_BUILD=1` — skip the build entirely (assume the artifact is already in
      place, or accept its absence). Used by the Docker image build, which compiles the extension
      in an earlier stage.
    * `FATHOM_UDF_PROFILE` — `release` (default) or `debug`.
  """

  use Mix.Task.Compiler

  @recursive false
  @crate_dir "native/fathom_udf"
  @priv_subdir "priv/sqlite_ext"

  @shortdoc "Builds the Django-compatibility SQLite extension"

  @impl Mix.Task.Compiler
  def run(_args) do
    cond do
      System.get_env("FATHOM_SKIP_UDF_BUILD") in ~w(1 true) ->
        :noop

      not File.dir?(@crate_dir) ->
        :noop

      is_nil(System.find_executable("cargo")) ->
        Mix.shell().info([
          :yellow,
          "fathom_udf: cargo not found — skipping the Django-compatibility SQLite extension. ",
          "Django's UDFs (__year, __date, Trunc*, __regex, …) will be unavailable. ",
          "Install Rust (https://rustup.rs) and re-run `mix compile` to enable them."
        ])

        :noop

      true ->
        build()
    end
  end

  defp build do
    profile = System.get_env("FATHOM_UDF_PROFILE", "release")
    args = if profile == "release", do: ["build", "--release"], else: ["build"]

    case System.cmd("cargo", args, cd: @crate_dir, stderr_to_stdout: true) do
      {_out, 0} ->
        install(profile)

      {out, status} ->
        # A compile ERROR in our own crate is a real failure and must not be papered over — this
        # is the case where the toolchain exists and the code is broken.
        Mix.shell().error(out)
        Mix.raise("fathom_udf: cargo build failed (exit #{status})")
    end
  end

  defp install(profile) do
    built = Path.join([@crate_dir, "target", profile, artifact_name()])

    if File.exists?(built) do
      File.mkdir_p!(@priv_subdir)
      dest = Path.join(@priv_subdir, artifact_name())

      # Copy only when the bytes actually changed. `mix compile` runs on every `mix test`, and
      # rewriting the file each time would churn its mtime for no reason.
      if stale?(built, dest) do
        File.cp!(built, dest)
        Mix.shell().info([:green, "fathom_udf: installed #{dest}"])
      end

      :ok
    else
      Mix.raise("fathom_udf: cargo reported success but #{built} is missing")
    end
  end

  defp stale?(src, dest) do
    not File.exists?(dest) or File.read!(src) != File.read!(dest)
  end

  @doc """
  The platform's shared-library name for the crate.

  macOS produces `.dylib`, Linux `.so`. The name is resolved rather than guessed at load time so
  `Fathom.Shard.Extension` and this task cannot disagree about it.
  """
  def artifact_name do
    case :os.type() do
      {:unix, :darwin} -> "libfathom_udf.dylib"
      {:win32, _} -> "fathom_udf.dll"
      _ -> "libfathom_udf.so"
    end
  end

  @impl Mix.Task.Compiler
  def clean do
    File.rm_rf(@priv_subdir)

    if File.dir?(@crate_dir) and System.find_executable("cargo") do
      System.cmd("cargo", ["clean"], cd: @crate_dir, stderr_to_stdout: true)
    end

    :ok
  end
end
