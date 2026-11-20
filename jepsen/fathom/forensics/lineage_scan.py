import re,sys,glob
st=sys.argv[1]
ev=[]
for f in glob.glob(st+'/node-logs/fathom*.log'):
    node=f.split('/')[-1][:-4]
    for line in open(f,errors='replace'):
        m=re.match(r'(\S+Z) .*shard (\S+): lock-trace (\S+) (.*)$',line)
        if m: ev.append((m.group(1),node,m.group(2),m.group(3),m.group(4))); continue
        m=re.match(r'(\S+Z) .*shard (\S+): open (REFUSED|FAILED) after the lease was acquired',line)
        if m: ev.append((m.group(1),node,m.group(2),'open_abandoned',''))
        m=re.match(r'(\S+Z) .*shard (\S+): lease release FAILED (.*)$',line)
        if m: ev.append((m.group(1),node,m.group(2),'release_failed',m.group(3)[:200]))
ev.sort()
rel={'release_delete','release_delete_by_read','release_if_owned_delete','release_delete_after_412','release_marker','put_marker'}
last={}; maxlin={}; flags=[]; pending={}; decision={}; unverified=[]; failed={}; decision_lock={}  # (shard,node) -> (claim, obj_stamp, stamped, t, kind)
def commit(sh):
    l,pn,pt=pending.pop(sh)
    if sh in maxlin and l<=maxlin[sh][0]:
        flags.append((pt,pn,sh,'LINEAGE-NOT-ABOVE-PRIOR-OPEN',l,'prior',maxlin[sh]))
    if sh not in maxlin or l>maxlin[sh][0]: maxlin[sh]=(l,pn,pt)
n=0
for t,node,sh,what,rest in ev:
    if what=='release_decision':
        cm=re.search(r'claim: (\d+)',rest)
        om=re.search(r'object: %\{([^}]*)\}',rest)
        stamp=None
        if om:
            nums=[int(x) for x in re.findall(r'(?:lineage|position_epoch): (\d+)',om.group(1))]
            stamp=max(nums) if nums else 0
        elif 'object: :absent' in rest: stamp=0
        om2=re.search(r'owner: "([^"]+)"',rest); em2=re.search(r'epoch: (\d+)',rest)
        # A CLEAN release of an unstamped open means that coordinator wrote nothing: no acked write, no
        # shipped frame, so its lineage was never an ownership and the next open may reuse the number
        # (hard-9 2026-11-19: fathom2 opened at 7, served nothing, released clean; fathom3 opened at 7).
        kind_m=re.search(r'kind: :(\w+)',rest); st_m=re.search(r'lineage_stamped: (\w+)',rest)
        if kind_m and kind_m.group(1) in ('clean','open_failed','abandoned_open') and st_m and st_m.group(1)=='false':
            if sh in pending and pending[sh][1]==node: pending.pop(sh)
        decision_lock[(sh,node)]=(om2.group(1) if om2 else None, int(em2.group(1)) if em2 else None)
        decision[(sh,node)]=(int(cm.group(1)) if cm else None, stamp, re.search(r'lineage_stamped: (\w+)',rest).group(1) if re.search(r'lineage_stamped: (\w+)',rest) else '?', t, re.search(r'kind: :(\w+)',rest).group(1) if re.search(r'kind: :(\w+)',rest) else '?')
        continue
    if what in ('release_delete','release_delete_by_read','release_if_owned_delete','release_delete_after_412') and (sh,node) in decision:
        c,stamp,stamped,dt,kind=decision.pop((sh,node))
        if c is not None and stamp is not None and stamp<c:
            flags.append((t,node,sh,'CLAIM-DELETED-BEFORE-STAMP','claim',c,'object_stamp',stamp,'lineage_stamped?',stamped,'kind',kind))
        elif c is not None and stamp is None:
            # The HEAD failed (S3 cut / nxdomain), so the stamp is unknown. With the flag false the delete
            # is already wrong; with it true it is unverifiable, so it is counted, never silently passed.
            if stamped=='false':
                flags.append((t,node,sh,'CLAIM-DELETED-UNSTAMPED-FLAG-FALSE','claim',c,'kind',kind))
            else:
                unverified.append((t,node,sh,c,kind,decision_lock.get((sh,node),(None,None))))
    if what=='release_failed':
        failed.setdefault((sh,node),[]).append((t,rest)); continue
    if what=='open_abandoned':
        # An open refused/failed after taking the lease served and shipped nothing: its lineage is
        # not an ownership, so a repeat of it is a retry, not a collision.
        pending.pop(sh,None); continue
    if what in('fresh_create','recreated') and sh in last and last[sh] not in rel:
        flags.append((t,node,sh,'LOCK-VANISHED',last[sh]))
    if what not in ('read_other','open_lineage'): last[sh]=what   # reads/opens do not change the lock
    if what=='open_lineage':
        n+=1
        if sh in pending: commit(sh)      # the previous open was not abandoned: it was served
        m=re.search(r'lineage: (\d+)',rest)
        if m: pending[sh]=(int(m.group(1)),node,t)
for sh in list(pending): commit(sh)
PRE_SEND=('nxdomain','econnrefused')   # the request never left this node, so the DELETE cannot have been applied
def _secs(t): return int(t[11:13])*3600+int(t[14:16])*60+float(t[17:23])
def _failure_pre_send(u):
    t,node,sh=u[0],u[1],u[2]
    fs=[(ft,txt) for ft,txt in failed.get((sh,node),[]) if ft>=t and _secs(ft)-_secs(t)<=90]
    return bool(fs) and all(any(p in txt for p in PRE_SEND) for ft,txt in fs)
def _lock_survived(u):
    """The first later lock event for the shard shows the same lock (owner+epoch) still there."""
    t,sh=u[0],u[2]; owner,epoch=u[5]
    for et,en,esh,ew,er in ev:
        if esh!=sh or et<=t: continue
        if ew=='steal':
            return ('from: "%s"'%owner in er) and ('from_epoch: %s,'%epoch in er or 'from_epoch: %s}'%epoch in er)
        if ew=='read_other':
            return ('owner: "%s"'%owner in er) and ('epoch: %s,'%epoch in er or 'epoch: %s}'%epoch in er)
        if ew in ('fresh_create','recreated'): return False
    return False
# A logged failure does NOT prove the delete missed: a timeout, an exit or a 5xx can arrive after the store
# applied it. Proven-not-landed = a pre-send failure (nxdomain / econnrefused) or the lock demonstrably survived.
unverified=[u for u in unverified if not (_failure_pre_send(u) or _lock_survived(u))]
print('opens=%d flags=%d unverified_deletes=%d'%(n,len(flags),len(unverified)))
for f in flags[:10]: print('  ',f)
