#!/usr/bin/env python3
"""Forensic timeline for ONE shard across everything a hard-mix run left behind.

usage: shard_timeline.py <store dir> <shard id or suffix, e.g. 11 or jep1794-11> [--trace lock-trace-NAME.log]
                         [--versions lock-versions-NAME.jsonl] [--from HH:MM:SS] [--to HH:MM:SS] [--all]

Merges, in UTC order: every node-log line naming the shard (lock-trace / lock-debug / warnings), the MinIO
`mc admin trace` lines for <shard>.lock (port 2100N = fathomN), and the versioned lock history
(isDeleteMarker rows and bodies). By default only lines that matter for lock/lineage forensics are shown;
--all shows every node-log line for the shard.

ORDERING: node logs, the MinIO trace and the lock history have independent clocks and sub-millisecond
rounding, so rows within a few ms of each other are a TIE, not a sequence (a MINIO PutObject can print
before the LOG line that issued it).
"""
import sys,re,glob,json,argparse
ap=argparse.ArgumentParser()
ap.add_argument('store'); ap.add_argument('shard')
ap.add_argument('--trace'); ap.add_argument('--versions')
ap.add_argument('--from',dest='t0'); ap.add_argument('--to',dest='t1'); ap.add_argument('--all',action='store_true')
a=ap.parse_args()
rows=[]
def keep(rest):
    return a.all or any(k in rest for k in ('lock-trace','lock-debug','takeover','PROMOTED','not promoting','lineage collision',
        'REFUSED','FORKED','lease release','lease superseded','heartbeat','fenc','recovery','handoff','drop','stopped'))
# a bare number means the shard-id suffix ("-12"), not any "12" in a line
shard_re=re.compile((r'-' if a.shard.isdigit() else r'')+re.escape(a.shard)+r'(?![0-9])')
for f in sorted(glob.glob(a.store+'/node-logs/fathom*.log')):
    node=f.split('/')[-1][:-4]
    for line in open(f,errors='replace'):
        m=re.match(r'(\S+Z) \S+ \[(\w+)\] (.*)$',line.rstrip())
        if not m or not shard_re.search(m.group(3)): continue
        if keep(m.group(3)): rows.append((m.group(1),node,'LOG',m.group(3)[:420]))
if a.trace:
    for line in open(a.trace,errors='replace'):
        m=re.match(r'(\d{4}-\d\d-\d\dT[\d:.]+) \[(\d+)[^\]]*\] s3\.(\w+) \S*:(\d+)/\S*/(\S+\.lock) (\S+)',line)
        if m and shard_re.search(m.group(5)):
            port=m.group(4); node='fathom'+port[-1] if port.startswith('2100') else 'ip '+m.group(6)
            rows.append((m.group(1)+'000Z',node,'MINIO',' '.join([m.group(3),m.group(2),m.group(5)])))
if a.versions:
    for line in open(a.versions):
        d=json.loads(line)
        if shard_re.search(d['key']):
            rows.append((d['lastModified'].replace('Z','000Z'),'(s3)','LOCKVER','DELETE-MARKER' if d['isDeleteMarker'] else d['body'][:300]))
rows.sort()
last_read=None; suppressed=0
for t,node,src,txt in rows:
    hms=t[11:23]
    if a.t0 and hms<a.t0: continue
    if a.t1 and hms>a.t1: continue
    # `read_other` is a poll of someone else's lock; an unchanged body is noise, a changed one is news
    m=re.search(r'lock-trace read_other (.*)$',txt)
    if m and not a.all:
        if m.group(1)==last_read: suppressed+=1; continue
        last_read=m.group(1)
    elif suppressed and not a.all:
        print('            ... %d unchanged read_other polls suppressed'%suppressed); suppressed=0
    print('%s %-8s %-7s %s'%(hms,node,src,txt))
if suppressed: print('            ... %d unchanged read_other polls suppressed'%suppressed)
