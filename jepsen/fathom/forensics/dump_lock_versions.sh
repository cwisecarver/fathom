#!/bin/zsh
# usage: dump_lock_versions.sh <out.jsonl>   (rig must still be up, MinIO bucket versioned)
# One JSON line per lock-object version: key, versionId, lastModified, isDeleteMarker, body.
out=$1; : >| $out
D=$(ls -d /opt/homebrew/Cellar/docker/*/bin | tail -1); export PATH="$D:$PATH"
M() { docker exec -i -e MC_CONFIG_DIR=/tmp/.mc fathom-chaos-minio-1 "$@"; }
M bash -c 'mc alias set local http://localhost:9000 fathom fathom-secret >/dev/null; mc ls --versions --recursive --json local/fathom-shards' > /tmp/lock_versions_listing.json 2>/dev/null
python3 - "$out" <<'PY' | M bash -s > /tmp/lock_versions_bodies.txt 2>/dev/null
import json,sys
print("mc alias set local http://localhost:9000 fathom fathom-secret >/dev/null")
for line in open('/tmp/lock_versions_listing.json'):
    try: d=json.loads(line)
    except Exception: continue
    k=d.get('key','')
    if not k.endswith('.lock'): continue
    vid=d.get('versionId','null'); dm=d.get('isDeleteMarker',False)
    print("echo '@@V %s %s %s %s'" % (k,vid,d.get('lastModified',''),dm))
    if not dm: print("mc cat --vid %s local/fathom-shards/%s 2>&1; echo" % (vid,k))
PY
python3 - "$out" <<'PY'
import json,sys,re
out=open(sys.argv[1],'w'); cur=None; body=[]
def flush():
    if cur: out.write(json.dumps({'key':cur[0],'versionId':cur[1],'lastModified':cur[2],'isDeleteMarker':cur[3]=='True','body':''.join(body).strip()})+'\n')
for line in open('/tmp/lock_versions_bodies.txt',errors='replace'):
    if line.startswith('@@V '):
        flush(); body=[]; cur=line[4:].strip().split(' ',3)
        if len(cur)<4: cur+=['']*(4-len(cur))
    else: body.append(line)
flush()
PY
echo "lock versions: $(wc -l < $out)"
