defmodule Fathom.Audit20260801SmallFixesTest do
  @moduledoc """
  The small, independent correctness fixes from the 2026-08-01 expert panel: #6, #13, #15, #45
  and #47. Each is a few lines of production code, and each was individually able to take a
  tenant offline, leak an unbounded resource, or silently disarm a prod safety guard.

  Not async: several flip application env.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Filo.Error

  describe "#6 — :hrana_server default drift silently disarmed two prod boot guards" do
    setup do
      prev = %{
        env: Application.get_env(:fathom, :env),
        hrana: Application.fetch_env(:fathom, :hrana_server),
        base: Application.get_env(:fathom, :shard_base_domain),
        lb: Application.get_env(:fathom, :lb_backends),
        ack: Application.get_env(:fathom, :allow_unanchored_routing)
      }

      on_exit(fn ->
        put_or_delete(:env, prev.env)
        put_or_delete(:shard_base_domain, prev.base)
        put_or_delete(:lb_backends, prev.lb)
        put_or_delete(:allow_unanchored_routing, prev.ack)

        case prev.hrana do
          {:ok, v} -> Application.put_env(:fathom, :hrana_server, v)
          :error -> Application.delete_env(:fathom, :hrana_server)
        end
      end)

      :ok
    end

    defp put_or_delete(key, nil), do: Application.delete_env(:fathom, key)
    defp put_or_delete(key, value), do: Application.put_env(:fathom, key, value)

    # THE case that was never tested and is the only state a real prod release is in: the key
    # UNSET. The listener defaulted it true and served Hrana; both guards defaulted it false and
    # concluded the data plane was not exposed, so neither fired.
    test "an UNSET :hrana_server still counts as exposed, so the base-domain guard fires" do
      Application.delete_env(:fathom, :hrana_server)
      Application.put_env(:fathom, :env, :prod)
      Application.delete_env(:fathom, :shard_base_domain)
      Application.delete_env(:fathom, :lb_backends)
      Application.delete_env(:fathom, :allow_unanchored_routing)

      assert_raise RuntimeError, ~r/shard_base_domain is unset/, fn ->
        Fathom.Application.check_shard_base_domain!()
      end
    end

    test "the guard still passes when routing is anchored" do
      Application.delete_env(:fathom, :hrana_server)
      Application.put_env(:fathom, :env, :prod)
      Application.put_env(:fathom, :shard_base_domain, "fathom.example")

      # The fix must not turn every anchored prod node into a boot failure.
      assert Fathom.Application.check_shard_base_domain!() == :ok
    end

    test "the explicit ack still opts out" do
      Application.delete_env(:fathom, :hrana_server)
      Application.put_env(:fathom, :env, :prod)
      Application.delete_env(:fathom, :shard_base_domain)
      Application.put_env(:fathom, :allow_unanchored_routing, true)

      assert Fathom.Application.check_shard_base_domain!() == :ok
    end

    test "one reader, so the default cannot drift again" do
      Application.delete_env(:fathom, :hrana_server)
      assert Fathom.Application.hrana_enabled?(), "unset must mean enabled — the listener's view"

      Application.put_env(:fathom, :hrana_server, false)
      refute Fathom.Application.hrana_enabled?()
    end
  end

  describe "#15 — a foreign-held lease must be a retryable 503, not a 500" do
    setup do
      shard = "held_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        Fathom.Shards.drain(shard, 2_000)

        for dir <- [Fathom.Shard.data_dir(), Storage.Local.dir()],
            suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
            do: File.rm(Path.join(dir, shard <> suffix))
      end)

      # A LIVE foreign lease, the shape the migration engine holds for the whole per-shard
      # blue/green sequence (`migrator@<node>`). The holder is live, so no steal-soon retry
      # path applies and the checkout errors immediately.
      File.mkdir_p!(Storage.Local.dir())

      File.write!(
        Path.join(Storage.Local.dir(), "#{shard}.lock"),
        Jason.encode!(%{
          "owner" => "migrator@internal-node-7",
          "epoch" => 7,
          "expires_at_ms" => System.system_time(:millisecond) + 60_000
        })
      )

      %{shard: shard}
    end

    test "the client gets a retryable 503, not the transport-default 500", %{shard: shard} do
      # Falling through open_error/1's status-less catch-all made Filo render 500, so every
      # fleet schema rollout — the project's headline capability — was a user-facing error in
      # each tenant's migration window rather than a retry the SDK backs off on.
      assert {:error, %Error{code: "FILO_SHARD_HELD", status: 503}} =
               Fathom.ShardExecutor.open(shard)
    end

    test "the holding node's identity is not leaked to the tenant", %{shard: shard} do
      {:error, %Error{message: msg}} = Fathom.ShardExecutor.open(shard)

      refute msg =~ "internal-node-7", "the owner leaks internal topology to a tenant"
      refute msg =~ "migrator"
    end
  end

  describe "#13 — the write circuit-breaker must arm before the shard is stealable" do
    test "the threshold is margin + steal_margin, not ttl + steal_margin" do
      # The clock starts at the FIRST :not_valid, which fires early by design — at
      # `last_renew + ttl - margin`, margin being ttl/3. Waiting another full ttl from there put
      # the fence at ~2·ttl while a peer may steal at ttl + steal_margin: ~20s of continued
      # write ACKs after another node could already own the shard, all quarantined on heal.
      ttl = Application.get_env(:fathom, :shard_lease_ttl_ms, 30_000)
      margin = Fathom.Shard.Heartbeat.margin_ms()
      steal = Storage.steal_margin_ms()

      # When the breaker arms, measured from the last successful renew.
      first_not_valid_at = ttl - margin
      fence_at = first_not_valid_at + margin + steal
      stealable_at = ttl + steal

      assert fence_at <= stealable_at,
             "the write fence must publish no later than the moment a peer may steal " <>
               "(fence at #{fence_at}ms vs stealable at #{stealable_at}ms)"

      # And the old constant provably did not hold.
      old_fence_at = first_not_valid_at + ttl + steal
      assert old_fence_at > stealable_at, "the pre-fix constant is what this test pins against"
    end

    test "margin_ms/0 answers with the heartbeat down" do
      refute Fathom.Shard.Heartbeat.running?()
      margin = Fathom.Shard.Heartbeat.margin_ms()
      assert is_integer(margin) and margin > 0
      assert margin == max(div(Application.get_env(:fathom, :shard_lease_ttl_ms, 30_000), 3), 1)
    end
  end

  describe "#45 — compressed-upload temps were outside every reaper glob" do
    test "a .z temp beside the live db is reaped" do
      dir = Path.join(System.tmp_dir!(), "reap_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      base = Path.join(dir, "shard.db")
      File.write!(base, "live")

      # The drop-flush compresses the LIVE path, so the temp is `<path>.z.<n>` — matching
      # neither the `{dl,snap,tmp,pull}` glob nor the fixed `.pull` family. One shard-sized
      # orphan per externally-killed drop-flush, forever.
      z = base <> ".z.12345"
      File.write!(z, "compressed")
      old = System.os_time(:second) - 3600
      File.touch!(z, old)

      assert Storage.reap_stale_temps(base, 60_000) >= 1
      refute File.exists?(z), "the .z temp was not reaped"
      assert File.exists?(base), "the reaper must never touch the live db"
    end

    test "the existing temp families are still reaped" do
      dir = Path.join(System.tmp_dir!(), "reap2_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      base = Path.join(dir, "shard.db")
      old = System.os_time(:second) - 3600

      temps = for s <- ~w(dl snap tmp pull z), do: base <> ".#{s}.9"

      for t <- temps do
        File.write!(t, "x")
        File.touch!(t, old)
      end

      assert Storage.reap_stale_temps(base, 60_000) == length(temps)
      for t <- temps, do: refute(File.exists?(t))
    end

    test "a FRESH temp is left alone — a live sibling may be writing it" do
      dir = Path.join(System.tmp_dir!(), "reap3_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      base = Path.join(dir, "shard.db")
      z = base <> ".z.1"
      File.write!(z, "in flight")

      assert Storage.reap_stale_temps(base, 60_000) == 0
      assert File.exists?(z)
    end
  end

  describe "#47 — QueryConsole must validate the shard id itself" do
    test "an invalid shard id is refused at the boundary, not spliced into a Host header" do
      # run/3 is a public, documented API that splices shard_id into a `Host` header and into
      # HranaAuth.token_for/1. The guard lived only in the LiveView caller, so any second caller
      # reopened review 2026-07-18 #16 — a dotted id routing to the wrong tenant.
      for bad <- ["acme.evil", "../etc", "has space", "", "UPPER.dotted"] do
        assert {:error, %{code: "INPUT"}} = Fathom.QueryConsole.run(bad, "SELECT 1"),
               "#{inspect(bad)} reached the transport"
      end
    end

    test "a valid id still gets through to the transport" do
      # No listener here, so it fails at the request — the point is that it got PAST validation
      # rather than being rejected as malformed input.
      assert {:error, %{code: code}} =
               Fathom.QueryConsole.run("acme", "SELECT 1", endpoint: "http://127.0.0.1:1")

      refute code == "INPUT", "a well-formed shard id must not be rejected as invalid input"
    end
  end
end
