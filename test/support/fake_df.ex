defmodule Fathom.Test.FakeDf do
  @moduledoc """
  A fake `df` first on `PATH` that records each invocation and blocks until released, for tests of
  what the OS sees when a `statfs` hangs (expert review 2026-11-07 #76: `Measurements.disk/0` and
  `Follower`'s seed-admission read must neither hold their caller nor stack a second `df` on a stuck
  one).

  `install!/1` is hermetic, in the ways the first version of `DiskPollBlockedTest` was not (it failed
  once in a full run, "the fake df was never started"; not reproduced in 150 isolated repeats, so the
  fixture was hardened against every actor that can start a `df` behind its back):

    * the live `Fathom.ShardPoller` runs `Measurements.disk/0` every 10 s and, with the fake on
      `PATH`, would start fake `df`s of its own. It is SUSPENDED for the test and resumed, after
      `PATH` is restored, at exit;
    * every other `df` caller (a follower refresh left over from an earlier test) starts its `df`
      with other arguments, so `calls/2` counts only invocations whose arguments contain a marker
      the test owns;
    * at exit the fake is released, and `PATH` comes back only once EVERY fake `df` that started has
      finished (a started/ended pair of files, not a process name), after the caller's `:settle`
      hook ran. A late `df` therefore never finds the real `PATH` half-restored, and a stuck fake
      never outlives its test.

  `:disk_poll_timeout_ms` is set for the test (default 300 ms) so a "hung" read gives up quickly.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Fathom.Test.{Await, Env}

  @type t :: %{dir: String.t(), started: String.t(), ended: String.t(), release: String.t()}

  @doc """
  Puts the fake on `PATH` for this test. Options: `:timeout_ms` (`:disk_poll_timeout_ms`, default
  300) and `:settle`, a zero-arity function run at exit after the fake is released and before
  `PATH` is restored (wait there for whatever process was running the `df`).
  """
  @spec install!(keyword()) :: t()
  def install!(opts \\ []) do
    dir = Path.join(System.tmp_dir!(), "fathom_fake_df_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    fake = %{
      dir: dir,
      started: Path.join(dir, "started"),
      ended: Path.join(dir, "ended"),
      release: Path.join(dir, "release")
    }

    # Records its arguments, blocks until released, then fails (so `volume_readings/1` answers
    # "unreadable" and the callers' fallbacks run).
    File.write!(Path.join(dir, "df"), """
    #!/bin/sh
    echo "$*" >> '#{fake.started}'
    while [ ! -f '#{fake.release}' ]; do sleep 0.02; done
    echo x >> '#{fake.ended}'
    exit 1
    """)

    File.chmod!(Path.join(dir, "df"), 0o755)

    poller = suspend_poller()
    old_path = System.get_env("PATH")
    System.put_env("PATH", dir <> ":" <> old_path)
    Env.put!(:disk_poll_timeout_ms, Keyword.get(opts, :timeout_ms, 300))
    settle = Keyword.get(opts, :settle, fn -> :ok end)

    on_exit(fn ->
      release!(fake)

      Await.until(
        fn -> length(lines(fake.started)) == length(lines(fake.ended)) end,
        "a fake df never exited; started: #{inspect(lines(fake.started))}",
        10_000
      )

      settle.()
      System.put_env("PATH", old_path)
      resume_poller(poller)
      File.rm_rf!(dir)
    end)

    fake
  end

  @doc "Lets every blocked fake `df` finish (it exits 1)."
  @spec release!(t()) :: :ok
  def release!(%{release: release}), do: File.write!(release, "")

  @doc """
  The argument lines of the fake `df` invocations so far, only those containing `marker` (default:
  all). Use a path the test owns as the marker, so a `df` started by anything else is not counted.
  """
  @spec calls(t(), String.t()) :: [String.t()]
  def calls(%{started: started}, marker \\ ""),
    do: Enum.filter(lines(started), &String.contains?(&1, marker))

  defp lines(path) do
    case File.read(path) do
      {:ok, body} -> String.split(body, "\n", trim: true)
      {:error, _} -> []
    end
  end

  defp suspend_poller do
    case Process.whereis(Fathom.ShardPoller) do
      nil ->
        nil

      pid ->
        :ok = :sys.suspend(pid)
        pid
    end
  end

  defp resume_poller(nil), do: :ok

  defp resume_poller(pid) do
    :sys.resume(pid)
  catch
    # The poller was restarted meanwhile; the new one was never suspended.
    :exit, _ -> :ok
  end
end
