#!/bin/sh
# Mounts LazyFS over /data for the LAZYFS=1 rig overlay. Runs as root, before the node drops to
# `app`. The real bytes live in /data-root (the container's own layer), so they survive a
# `docker kill` + `docker start`; whatever was not fsynced lived only in lazyfs and does not.
set -eu
root=/data-root
# First boot: seed the backing dir with the image's /data skeleton (shards/, warm/, owned by app).
if [ ! -e "$root.seeded" ]; then
  mkdir -p "$root"
  cp -a /data/. "$root/"
  touch "$root.seeded"
fi
# The node's stand-in for the kernel boot id (REPLICATION_BOOT_ID_PATH): every container shares the
# VM's real one, so a container restart can never look like a reboot. The Jepsen :power-loss fault
# overwrites this file while the node is down; created once here, so a plain restart keeps it.
mkdir -p /var/lib/fathom
[ -s /var/lib/fathom/boot_id ] || cat /proc/sys/kernel/random/uuid > /var/lib/fathom/boot_id
chmod 644 /var/lib/fathom/boot_id
rm -f /tmp/lazyfs.fifo
lazyfs /data --config-path /etc/lazyfs.toml -o allow_other -o modules=subdir -o subdir="$root"
i=0
until mountpoint -q /data; do
  i=$((i + 1))
  [ "$i" -lt 100 ] || { echo "lazyfs did not mount /data" >&2; exit 1; }
  sleep 0.1
done
echo "lazyfs mounted on /data (backing $root)"
