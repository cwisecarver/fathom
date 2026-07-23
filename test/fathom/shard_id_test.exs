defmodule Fathom.ShardIdTest do
  # The shard-id validator is the shard-isolation + path-traversal gate. This test
  # pins the invariant so a future loosening is a deliberate, visible change — the
  # bug the DRY collapse (one validator, not two copies) exists to prevent.
  use ExUnit.Case, async: true

  alias Fathom.ShardId

  describe "valid?/1 accepts" do
    test "alphanumerics, underscore, hyphen" do
      assert ShardId.valid?("acme")
      assert ShardId.valid?("tenant_42")
      assert ShardId.valid?("a-b-c")
      assert ShardId.valid?("ABC123")
      assert ShardId.valid?("x")
    end

    test "the full 64-char length" do
      assert ShardId.valid?(String.duplicate("a", 64))
    end
  end

  describe "valid?/1 rejects" do
    test "empty and over-length" do
      refute ShardId.valid?("")
      refute ShardId.valid?(String.duplicate("a", 65))
    end

    test "path traversal and separators" do
      refute ShardId.valid?("..")
      refute ShardId.valid?("../etc")
      refute ShardId.valid?("a/b")
      refute ShardId.valid?("a.b")
      refute ShardId.valid?("a\\b")
    end

    test "whitespace and control characters" do
      refute ShardId.valid?("a b")
      refute ShardId.valid?("a\tb")
      refute ShardId.valid?("a\nb")
      refute ShardId.valid?(" acme")
    end

    test "non-binaries (untrusted request values pass straight in)" do
      refute ShardId.valid?(nil)
      refute ShardId.valid?(:atom)
      refute ShardId.valid?(123)
      refute ShardId.valid?(["acme"])
    end
  end

  # Finding #19: cast normalizes case so `ACME` and `acme` name the same shard.
  describe "cast/1" do
    test "downcases a valid id to canonical form" do
      assert ShardId.cast("ACME") == {:ok, "acme"}
      assert ShardId.cast("Tenant_42") == {:ok, "tenant_42"}

      assert ShardId.cast("acme") == {:ok, "acme"},
             "already-canonical ids are unchanged (idempotent)"

      assert ShardId.cast("ABC-123") == {:ok, "abc-123"}
    end

    test "is idempotent: casting a cast id is a no-op" do
      {:ok, once} = ShardId.cast("MixedCase")
      assert ShardId.cast(once) == {:ok, once}
    end

    test "rejects what valid?/1 rejects" do
      assert ShardId.cast("../etc") == :error
      assert ShardId.cast("a/b") == :error
      assert ShardId.cast("") == :error
      assert ShardId.cast(123) == :error
      assert ShardId.cast(nil) == :error
    end

    test "the result is always itself valid? (validity invariant under downcase)" do
      for id <- ["ACME", "Z9", "A_B-C", String.duplicate("A", 64)] do
        assert {:ok, canonical} = ShardId.cast(id)
        assert ShardId.valid?(canonical)
      end
    end
  end

  # Expert review #35: dns_safe?/1 is a SUPERSET gate over valid?/1 (never a replacement) — the
  # wildcard-TLS servability check the one address-composing site (Tenants.provision/fork) uses.
  describe "dns_safe?/1" do
    test "accepts DNS-safe labels (letters, digits, hyphens)" do
      assert ShardId.dns_safe?("acme")
      assert ShardId.dns_safe?("acme-preview")
      assert ShardId.dns_safe?("a-b-c-123")
      assert ShardId.dns_safe?("x")
      assert ShardId.dns_safe?(String.duplicate("a", 63)), "63 chars is the DNS-label max"
    end

    test "rejects an underscore id (RFC 6125 — no wildcard-TLS match) even though valid?/1 admits it" do
      assert ShardId.valid?("tenant_42"), "the isolation validator stays permissive"
      refute ShardId.dns_safe?("tenant_42"), "but it isn't wildcard-TLS-servable"
    end

    test "rejects over-63-char, leading/trailing hyphen, and every non-valid? id" do
      refute ShardId.dns_safe?(String.duplicate("a", 64)),
             "valid? but >63 chars = not a DNS label"

      refute ShardId.dns_safe?("-lead")
      refute ShardId.dns_safe?("trail-")
      refute ShardId.dns_safe?("a/b")
      refute ShardId.dns_safe?("")
      refute ShardId.dns_safe?(nil)
      refute ShardId.dns_safe?(123)
    end
  end
end
