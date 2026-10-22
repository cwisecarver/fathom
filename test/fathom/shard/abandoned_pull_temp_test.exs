defmodule Fathom.Shard.AbandonedPullTempTest do
  # Regression, rig 2026-10-14 N4. Chaos-rig symptom: a node was killed and clients retried every
  # ~3 s while the dead owner's lease was still held. Each retry started a coordinator whose cold
  # pull (overlapped with `acquire_lease`) was abandoned when the acquire answered `:held`, and
  # `Materializer.abandon_pull/2` brutally killed the task. That skips `S3.download`'s try/after,
  # so the streaming temp `<shard>.db.pull.dl.<n>` was stranded: 7-14 per tenant, 14 GB -> 7.5 GB
  # of free disk across 100 tenants. `abandon_pull/2` only removed `<path>.pull{,-wal,-shm}`.
  #
  # Invariant: abandoning an in-flight pull leaves NO `*.pull.dl.*` (or any pull temp) behind.
  # This drives the REAL S3 backend against an in-process server that stalls mid-body, because
  # `Storage.Local` never creates a `.dl.` temp and so cannot express the bug.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Materializer
  alias Fathom.Shard.Storage.S3

  defmodule SlowBody do
    @moduledoc false
    @behaviour Plug
    @impl true
    def init(opts), do: opts

    # Sends the headers + one data chunk (so the client has created its temp), tells the test,
    # then stalls until the client hangs up.
    @impl true
    def call(conn, _opts) do
      conn = Plug.Conn.put_resp_header(conn, "etag", "\"d41d8cd98f00b204e9800998ecf8427e\"")
      conn = Plug.Conn.send_chunked(conn, 200)
      {:ok, conn} = Plug.Conn.chunk(conn, :binary.copy(<<0>>, 4096))
      send(:persistent_term.get({__MODULE__, :test}), :first_chunk_sent)
      Process.sleep(30_000)
      conn
    end
  end

  setup do
    :persistent_term.put({SlowBody, :test}, self())
    on_exit(fn -> :persistent_term.erase({SlowBody, :test}) end)

    {:ok, server} = Bandit.start_link(plug: SlowBody, port: 0, ip: :loopback)
    {:ok, {_, port}} = ThousandIsland.listener_info(server)

    prev_s3 = Application.get_env(:fathom, S3)
    prev_storage = Application.get_env(:fathom, :shard_storage)

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      endpoint: "http://127.0.0.1:#{port}",
      path_style: true,
      prefix: "p/",
      access_key_id: "k",
      secret_access_key: "s"
    )

    Application.put_env(:fathom, :shard_storage, S3)

    on_exit(fn ->
      if prev_s3,
        do: Application.put_env(:fathom, S3, prev_s3),
        else: Application.delete_env(:fathom, S3)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)
    end)

    dir =
      Path.join(System.tmp_dir!(), "fathom_abandon_pull_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{path: Path.join(dir, "shard.db"), dir: dir}
  end

  test "abandon_pull on an in-flight S3 download leaves no pull temp on disk", %{
    path: path,
    dir: dir
  } do
    task = Materializer.start_pull("abandoned_pull_shard", path)
    assert_receive :first_chunk_sent, 10_000

    # Precondition: the download really is mid-flight with its temp on disk. Without this the
    # test would pass vacuously (nothing was ever written). The temp is created on the first data
    # chunk, which the client may still be flushing when the server's notice arrives.
    wait_for_dl(dir)

    :ok = Materializer.abandon_pull(task, path)

    assert File.ls!(dir) == [], "abandoned pull stranded temps: #{inspect(File.ls!(dir))}"
  end

  # The window the test above hits only rarely (2026-10-15, once in a full suite run): S3.download
  # creates `<pull>.dl.<n>` with File.open and records it in the task's process dictionary a moment
  # LATER, so a kill in between found no name to remove and the temp survived. Made deterministic
  # here: the temp exists on disk and the task's dictionary does not name it. Invariant: no
  # `<pull>.dl.*` survives abandon_pull, whatever the dictionary says.
  test "abandon_pull removes a download temp the task had not recorded yet", %{
    path: path,
    dir: dir
  } do
    unrecorded = Materializer.pull_temp(path) <> ".dl.2"
    File.write!(unrecorded, "partial body")

    task =
      Task.async(fn ->
        receive do
          :never -> :ok
        end
      end)

    :ok = Materializer.abandon_pull(task, path)

    assert File.ls!(dir) == [], "abandoned pull stranded temps: #{inspect(File.ls!(dir))}"
  end

  defp wait_for_dl(dir, tries \\ 100) do
    cond do
      Path.wildcard(Path.join(dir, "shard.db.pull.dl.*")) != [] ->
        :ok

      tries == 0 ->
        flunk("the fixture did not leave an in-flight download temp")

      true ->
        Process.sleep(20)
        wait_for_dl(dir, tries - 1)
    end
  end
end
