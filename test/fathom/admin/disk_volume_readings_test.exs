defmodule Fathom.Admin.DiskVolumeReadingsTest do
  @moduledoc """
  One `df` for every directory the disk gauge reports, bytes and inodes together (expert review
  2026-10-22 r8 #10). The tick used to run `disksup` (a `df` through its port) plus `df -Pi` per
  directory: four subprocesses per poll on a follower node.

  The parser is the risk: a misread column is a wrong free-space number, and that number gates
  seed admission. So every shape it does not understand must come back `nil` (the caller then
  falls back to the old per-directory pair), never a guess.
  """
  use ExUnit.Case, async: true

  alias Fathom.Admin.Measurements

  describe "parse_df_volumes/2" do
    test "GNU --output=size,avail,itotal,iavail,target, one row per argument" do
      out = """
      1B-blocks       Avail  Inodes   IFree Mounted on
      107374182400 53687091200 6553600 3600 /var/lib/fathom
      107374182400 53687091200 6553600 3600 /var/lib/fathom
      """

      assert [{:ok, r}, {:ok, r}] = Measurements.parse_df_volumes(out, :gnu)

      assert r == %{
               mount: "/var/lib/fathom",
               total_bytes: 107_374_182_400,
               free_bytes: 53_687_091_200,
               total_inodes: 6_553_600,
               free_inodes: 3600
             }
    end

    test "GNU: a mount point with spaces is kept whole" do
      out = "1B-blocks Avail Inodes IFree Mounted on\n100 40 10 5 /mnt/my disk\n"

      assert [{:ok, %{mount: "/mnt/my disk", total_bytes: 100, free_bytes: 40}}] =
               Measurements.parse_df_volumes(out, :gnu)
    end

    test "BSD df -Pki: 1024-blocks and Available are KiB, inodes are iused + ifree" do
      out = """
      Filesystem   1024-blocks       Used  Available Capacity iused       ifree %iused  Mounted on
      /dev/disk3s5  3902614160 2559061812 1308229604    67% 8954767 13082296040    0%   /System/Volumes/Data
      """

      assert [{:ok, r}] = Measurements.parse_df_volumes(out, :bsd)
      assert r.total_bytes == 3_902_614_160 * 1024
      assert r.free_bytes == 1_308_229_604 * 1024
      assert r.total_inodes == 8_954_767 + 13_082_296_040
      assert r.free_inodes == 13_082_296_040
      assert r.mount == "/System/Volumes/Data"
    end

    test "BSD: a row whose field count does not match the header is unreadable, not shifted" do
      # A filesystem name with a space would shift every column one to the right.
      out = """
      Filesystem   1024-blocks       Used  Available Capacity iused       ifree %iused  Mounted on
      map auto_home          0          0          0   100%       0           0  100%   /System/Volumes/Data/home
      """

      assert [nil] = Measurements.parse_df_volumes(out, :bsd)
    end

    test "no fixed inode table (btrfs reports 0) gives nil inodes, not 0 free" do
      out = "1B-blocks Avail Inodes IFree Mounted on\n100 40 0 0 /data\n"

      assert [{:ok, %{total_inodes: nil, free_inodes: nil, free_bytes: 40}}] =
               Measurements.parse_df_volumes(out, :gnu)

      out = "1B-blocks Avail Inodes IFree Mounted on\n100 40 - - /data\n"

      assert [{:ok, %{total_inodes: nil, free_inodes: nil}}] =
               Measurements.parse_df_volumes(out, :gnu)
    end

    test "garbage and errors are unreadable" do
      assert [] = Measurements.parse_df_volumes("", :gnu)
      assert [nil] = Measurements.parse_df_volumes("1B-blocks Avail\nx y z w /m\n", :gnu)

      assert [nil] =
               Measurements.parse_df_volumes("Filesystem 1024-blocks\n/dev/x 12 /m\n", :bsd)
    end
  end

  describe "volume_readings/1 on this host" do
    test "one reading per path, in order, agreeing with disksup's byte total" do
      tmp = System.tmp_dir!()
      missing = Path.join(tmp, "fathom_absent_#{System.unique_integer([:positive])}")

      assert [{:ok, a}, {:ok, b}] = Measurements.volume_readings([tmp, missing]),
             "precondition: this host's df is one the parser reads"

      assert a.total_bytes == b.total_bytes, "a missing dir must report its existing ancestor"
      assert a.free_bytes > 0 and a.free_bytes <= a.total_bytes

      case Measurements.disk_info(tmp) do
        {:ok, %{total_bytes: total}} -> assert a.total_bytes == total
        # os_mon not running in this env: nothing to cross-check.
        :error -> :ok
      end
    end

    test "no paths, no subprocess" do
      assert [] = Measurements.volume_readings([])
    end
  end
end
