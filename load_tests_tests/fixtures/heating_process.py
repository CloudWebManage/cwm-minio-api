"""Synthetic worker interpreter; patch sockets before importing campaign code."""
from gevent.monkey import patch_all
patch_all()

import base64
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from conftest import MemoryS3
from cwm_minio_api.load_tests.campaign.config import Manifest
from cwm_minio_api.load_tests.campaign import s3 as adapter
from cwm_minio_api.load_tests.campaign.heating import worker_heat

payload=json.load(sys.stdin)
s3=MemoryS3();s3.buckets=payload['buckets'];s3.versioning=payload['versioning'];s3.corrupt=payload.get('corrupt',False)
for bucket in s3.buckets.values():
    for versions in bucket.values():
        for version in versions:
            if 'Body' in version:version['Body']=base64.b64decode(version['Body'])
original=s3.head_object
s3.head_object=lambda **kw:dict(original(**kw),StorageClass='LOW')
adapter.client=lambda *a:s3
try:
    result=worker_heat(None,Manifest.model_validate(payload['manifest']),payload['data'],payload['worker'],payload['root'],payload['session'])
    print(json.dumps({'status':'ok','result':result}))
except BaseException as exc:
    print(json.dumps({'status':'error','error_type':type(exc).__name__,'reason':str(exc)}))
