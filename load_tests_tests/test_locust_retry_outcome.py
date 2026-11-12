"""Exercise the real adapter in a subprocess to isolate gevent monkey patches."""
import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize('mode', ['recovered', 'unrecovered', 'fatal', 'repeat-fail'])
def test_adapter_retry_completion(tmp_path, mode):
    script=r'''
from cwm_minio_api.load_tests.campaign import locustfile as a
from types import SimpleNamespace as N
from pathlib import Path
import json,os,gevent
quits=[]
e=N(runner=N(quit=lambda:quits.append(1),exceptions={}),process_exit_code=None)
a.ctx.clear()
if os.environ['MODE']=='repeat-fail':
    a.fail(e,'original-cause');a.fail(e,'secondary-cause');gevent.sleep(0)
    assert quits==[1] and a.ctx['failure']=='original-cause'
else:
    a.ctx.update(coord=None,failure='checksum mismatch' if os.environ['MODE']=='fatal' else None,
      request_failures=1,request_count=4,stage='mixed',attempt='attempt',
      store=N(recovered_read_errors=1 if os.environ['MODE']=='recovered' else 0),
      state=N(sequence_counts=lambda *args:dict(admitted_sequences=1,completed_sequences=1,incomplete_sequences=0)))
    os.environ['CWM_CAMPAIGN_OUTCOME']=os.environ['OUTCOME']
    a.finished(e)
    row=json.loads(Path(os.environ['OUTCOME']).read_text())
    assert row['status']==('passed' if os.environ['MODE']=='recovered' else 'failed')
    assert row['physical_request_errors']==1
    assert e.process_exit_code==(0 if row['status']=='passed' else 1)
'''
    result=subprocess.run([sys.executable,'-c',script],env={**os.environ,'MODE':mode,'OUTCOME':str(tmp_path/'outcome.json')},capture_output=True,text=True)
    assert result.returncode==0,result.stderr
