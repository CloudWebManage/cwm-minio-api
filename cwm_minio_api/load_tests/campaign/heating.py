"""Exact-version distributed heating. Allocated work is never replayed implicitly."""
import json
import math
import re
import threading
import time
import uuid

from .budget import BudgetExhausted
from .config import Aborted, CampaignError, Inconclusive
from .tier import TierStages, is_storage_class


def validate_task(m, rec):
    try:
        role=rec['bucket'].rsplit('-',1)[1]
        index=int(rec['key'].removeprefix('seed/'))
        valid=(role in ('cold','expiry') and rec['bucket']==m.bucket(role)
               and rec['key']==f'seed/{index:06d}' and 0<=index<m.dataset.objects
               and rec['size']==m.dataset.sizes[index%len(m.dataset.sizes)]
               and type(rec['size']) is int and rec['kind']=='put' and rec['generation']==0
               and isinstance(rec['op'],str) and bool(rec['op'])
               and isinstance(rec['version_id'],str) and bool(rec['version_id'])
               and re.fullmatch('[0-9a-f]{64}',rec['sha256']))
        if not valid:raise ValueError()
    except (ValueError,KeyError,TypeError,AttributeError,IndexError):
        raise CampaignError('invalid distributed heat task identity') from None


def validate_receipt(m, rec, receipt):
    validate_task(m,rec)
    try:
        gate=receipt['gate'];started=gate['started'];finished=receipt['finished'];head_at=receipt['head_at']
        if any(type(v) not in (int,float) or not math.isfinite(v) for v in (started,finished,head_at)):
            raise ValueError()
        hour=int(started//3600)
        expected=dict(gate='heat',status='passed',utc_hour=hour,high_hours=m.tier.high_hours,
                      high_include_current=m.tier.high_include_current,
                      eligible_at=started if m.tier.high_include_current else (hour+1)*3600,
                      window_expires_at=(hour+m.tier.high_hours+(not m.tier.high_include_current))*3600,
                      current_gets=max(4,m.tier.high_threshold+1),version_gets=1)
        if (receipt['op']!=rec['op'] or receipt['version_id']!=rec['version_id']
            or not 0<=started<=head_at<=finished<(hour+1)*3600
            or any(gate.get(k)!=v or type(gate.get(k)) is not type(v) for k,v in expected.items())):
            raise ValueError()
        head=receipt['head']
        if (type(head.get('ContentLength')) is not int or head.get('ContentLength')!=rec['size']
            or head.get('VersionId','null')!=rec['version_id']
            or head.get('Metadata')!={'cwm-op':rec['op'],'cwm-sha256':rec['sha256'],'cwm-run':m.run_id}):
            raise ValueError()
    except (ValueError,KeyError,TypeError):
        raise CampaignError('invalid or incomplete distributed heat receipt') from None


def import_receipts(m,state,tasks,receipts):
    if len({r['op'] for r in tasks})!=len(tasks) or set(receipts)!={r['op'] for r in tasks}:
        raise CampaignError('distributed heating lacks exact complete receipts')
    # Validate all before publishing any passing gates.
    for rec in tasks:validate_receipt(m,rec,receipts[rec['op']])
    for rec in tasks:
        r=receipts[rec['op']];role=rec['bucket'].rsplit('-',1)[1]
        state.observe('heat',role,rec,'head',r['head'],r['head_at'])
        state.observe('heat',role,rec,'gate',r['gate'],r['finished'])


def master_heat(store,coord):
    if not coord:raise CampaignError('distributed heating requires coordination')
    tier=TierStages(store);tier.start('heat')
    for role in ('cold','quiet','expiry'):
        for rec in tier.records(role):tier.prerequisite('cold',rec)
    tasks=[rec for role in ('cold','expiry') for rec in tier.records(role)]
    for rec in tasks:validate_task(store.m,rec)
    session=coord.begin('heat');started=coord.now()
    end=started+store.m.coordination.rendezvous_seconds+store.m.limits.duration_seconds
    coord.heat_initialize([json.dumps(r) for r in tasks],started,end)
    result=None
    try:
        rendezvous=started+store.m.coordination.rendezvous_seconds
        while coord.retry(lambda:coord.redis.hlen(session+':workers'))<store.m.coordination.expected_workers:
            coord.guard();tier.sample_quiet()
            if coord.now()>=rendezvous:raise Aborted('heating worker rendezvous deadline reached')
            time.sleep(.2)
        if coord.retry(lambda:coord.redis.hlen(session+':workers'))!=store.m.coordination.expected_workers:
            raise CampaignError('heating worker count mismatch')
        coord.retry(lambda:coord.redis.hset(session,'phase','running'))
        while coord.retry(lambda:coord.redis.hlen(session+':heat-receipts'))<len(tasks):
            coord.guard();tier.sample_quiet()
            if coord.now()>=end:raise Inconclusive('distributed heating deadline reached; partial heating is not replayed')
            time.sleep(.2)
        coord.guard();tier.sample_quiet(force=True)
        receipts={k:json.loads(v) for k,v in coord.retry(lambda:coord.redis.hgetall(session+':heat-receipts')).items()}
        for r in receipts.values():
            if not started<=r['gate']['started']<=r['finished']<=end:
                raise CampaignError('heating receipt outside master session')
        import_receipts(store.m,store.state,tasks,receipts)
        result=dict(heated_objects=len(tasks),workers=store.m.coordination.expected_workers,
                    heating_gets_per_key=max(4,store.m.tier.high_threshold+1),
                    version_specific_cold_reads_per_key=1,session=session)
        return result
    finally:
        coord.retry(lambda:coord.redis.hset(session,mapping={'phase':'ended','heat_status':'passed' if result else 'failed'}))
        coord.collect();coord.session=None


def worker_heat(bundle,m,data,worker,state_dir,session):
    from pathlib import Path
    from .config import check_path
    from .distributed import Coordinator
    from .report import Metrics
    from .s3 import ObjectStore,client
    from .state import State
    root=Path(state_dir)
    if not root.is_absolute() or '..' in root.parts:raise CampaignError('invalid worker state root')
    check_path(root)
    state=State(m,path=root/m.run_id/worker/uuid.uuid4().hex)
    coord=Coordinator(m,state,data['runtime']['endpoint'],data['redis_url'],incarnation=data['incarnation'])
    prefix=coord.prefix+':session:'
    if not isinstance(session,str) or not session.startswith(prefix) or not re.fullmatch('[0-9a-f]{32}',session[len(prefix):]):
        raise CampaignError('heating requires exact master session identity')
    if coord.retry(lambda:coord.redis.hget(session,'stage'))!='heat':raise CampaignError('master session is not heating')
    coord.join(worker,session)
    from gevent import monkey
    cooperative=monkey.is_module_patched('socket')
    if cooperative:
        import gevent
        from gevent.event import Event
        from gevent.pool import Pool
        stop=Event();lost=Event()
        concurrency=max(1,m.limits.users//m.coordination.expected_workers)
        pool=Pool(concurrency)
    else:
        stop=threading.Event();lost=threading.Event();concurrency=1;pool=None
    def heartbeat():
        while not stop.wait(1):
            try:coord.heartbeat_worker(worker)
            except Exception:
                try:
                    ended=coord.retry(lambda:coord.redis.hget(session,'phase'))=='ended'
                except Exception:
                    ended=False
                if not ended:lost.set()
                return
    if cooperative:
        thread=gevent.spawn(heartbeat)
    else:
        thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
    def guard():
        if lost.is_set():raise Aborted('heating worker heartbeat lost')
        coord.guard()
    metric=Metrics(state,coord.publish_metric,attempt=session,phase='heat')
    store=ObjectStore(m,state,client(m,data['runtime']),coord,metric,guard);store.stage='heat'
    tier=TierStages(store);tier.stage='heat'
    # Only controller samples quiet; worker never receives quiet object tasks.
    tier.sample_quiet=lambda force=False:None
    completed=0
    failures=[]
    def consume(rec):
        nonlocal completed
        try:
            validate_task(m,rec)
            head=store.head(rec,current=True)
            if not is_storage_class(head,m.tier.storage_class) or head.get('Restore'):
                raise CampaignError('distributed heating requires confirmed cold current version')
            tier.heat_object(rec,True)
            metadata=state.get('metadata:'+rec['op']);gate=state.get('gate:heat:'+rec['op'])
            receipt=dict(op=rec['op'],version_id=rec['version_id'],finished=gate['ts'],
                         gate=gate['value'],head=metadata['value'],head_at=metadata['ts'])
            validate_receipt(m,rec,receipt);guard()
            coord.heat_receipt(rec['op'],json.dumps(receipt))
            completed+=1
        except BaseException as exc:
            failures.append(exc)
    try:
        while True:
            if failures:raise failures[0]
            phase=coord.retry(lambda:coord.redis.hget(session,'phase'))
            if phase=='ended':
                if coord.retry(lambda:coord.redis.hget(session,'heat_status'))!='passed':raise Inconclusive('master heating session did not pass')
                if pool is not None:pool.join(timeout=5,raise_error=True)
                if failures or (pool is not None and len(pool)):
                    raise Inconclusive('heating tasks did not drain after master completion')
                return dict(worker=worker,heated_objects=completed,concurrency=concurrency,session=session)
            try:
                guard()
            except Aborted:
                # Completion may occur after the phase snapshot above. Re-enter
                # the terminal branch, which requires a passing exact-session
                # receipt and drains local tasks before returning success.
                if coord.retry(lambda:coord.redis.hget(session,'phase'))=='ended':
                    continue
                raise
            if phase!='running' or (pool is not None and pool.free_count()==0):
                time.sleep(.05);continue
            raw=coord.heat_take()
            if raw is None:time.sleep(.05);continue
            # Allocation is at-most-once, including failure after a cold GET.
            rec=json.loads(raw)
            if pool is None:consume(rec)
            else:pool.spawn(consume,rec)
    except BaseException as exc:
        try:
            if coord.retry(lambda:coord.redis.hget(session,'phase'))!='ended':
                reason='BudgetExhausted' if isinstance(exc,BudgetExhausted) else 'heating worker incomplete; inspect retained receipts'
                coord.retry(lambda:coord.redis.hset(session+':failures',worker,reason))
        except Exception:
            pass  # Preserve the original failure if reporting is unavailable.
        raise
    finally:
        stop.set()
        if pool is not None:pool.kill(block=True,timeout=5)
        thread.join(timeout=5)
        state.close()
