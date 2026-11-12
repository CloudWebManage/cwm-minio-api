import sys
if len(sys.argv)>1:
 from gevent import monkey
 monkey.patch_all()
import time,json,signal
from types import SimpleNamespace
from botocore.httpsession import URLLib3Session
from botocore.awsrequest import AWSRequest
from cwm_minio_api.load_tests.campaign.heating import HeatObserver
from cwm_minio_api.load_tests.campaign.tier import TierStages
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
class SlowConnection:
 def urlopen(self,**kwargs):time.sleep(10);raise AssertionError('deadline ignored')
http=URLLib3Session();http._get_connection_manager=lambda *args:SimpleNamespace(connection_from_url=lambda url:SlowConnection());http._setup_ssl_cert=lambda *args:None
rec=dict(op='object',bucket='run-cold',key='seed/000000',version_id='version',size=1,sha256='0'*64)
observations=[]
m=SimpleNamespace(run_id='run',limits=SimpleNamespace(request_timeout_seconds=30),tier=SimpleNamespace(storage_class='LOW',poll_seconds=1))
metadata={}
def observe(*args):
 observations.append(args)
 metadata['metadata:'+args[2]['op']]={'seq':len(observations),'value':args[4]}
state=SimpleNamespace(stopped=lambda:False,observe=observe,get=lambda key:metadata.get(key))
sdk=SimpleNamespace(head_object=lambda **kwargs:http.send(AWSRequest(method='HEAD',url='https://offline.invalid').prepare()))
store=ObjectStore(m,state,sdk)
tier=TierStages(store);tier.stage='heat'
coord=SimpleNamespace(guard=lambda:None,now=time.time,retry=lambda fn:fn(),redis=SimpleNamespace(hscan=lambda *a,**kw:(0,{'x':json.dumps(rec)})))
observer=HeatObserver(tier,coord,'session',[rec],time.time()+10)
start=time.monotonic();observer.sample();elapsed=time.monotonic()-start
assert elapsed<3,elapsed
assert len(observations)==1 and observations[-1][4]['state']=='unavailable',observations
assert len(observer.pending)==1
if len(sys.argv)==1:assert signal.getitimer(signal.ITIMER_REAL)==(0.0,0.0)
time.sleep(.25)
print(json.dumps(dict(mode='gevent' if len(sys.argv)>1 else 'native',elapsed=elapsed,state='unavailable',pending=1)))
