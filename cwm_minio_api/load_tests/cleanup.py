import os
import json
import requests
import subprocess
from textwrap import dedent

from ..minio import api as minio_api
from ..config import MINIO_MC_PROFILE
from .config import CWM_MINIO_API_HOST, CWM_MINIO_API_USERNAME, CWM_MINIO_API_PASSWORD


def is_valid_id_for_cleanup(txt):
    if '-' not in txt:
        return False
    parts = txt.split('-')
    if len(parts) != 6:
        return False
    valid_prefix = False
    for prefix in ['cmalti', 'cmaltbpriv', 'cmaltbpub']:
        if parts[0] == prefix:
            valid_prefix = True
            break
    if not valid_prefix:
        return False
    return True


async def main():
    for instance_id in requests.get(f'https://{CWM_MINIO_API_HOST}/instances/list', auth=(CWM_MINIO_API_USERNAME, CWM_MINIO_API_PASSWORD)).json():
        if instance_id.startswith('cmalti-') and is_valid_id_for_cleanup(instance_id):
            print(instance_id)
            for bucket_name in requests.get(f'https://{CWM_MINIO_API_HOST}/buckets/list?instance_id={instance_id}', auth=(CWM_MINIO_API_USERNAME, CWM_MINIO_API_PASSWORD)).json():
                if (bucket_name.startswith('cmaltbpriv-') or bucket_name.startswith('cmaltbpub-')) and is_valid_id_for_cleanup(bucket_name):
                    print(bucket_name)
                    res = requests.delete(f'https://{CWM_MINIO_API_HOST}/buckets/delete?instance_id={instance_id}&bucket_name={bucket_name}', auth=(CWM_MINIO_API_USERNAME, CWM_MINIO_API_PASSWORD))
                    if res.status_code != 200:
                        print(f'Failed to delete bucket: {res.status_code} {res.text}')
            res = requests.delete(f'https://{CWM_MINIO_API_HOST}/instances/delete?instance_id={instance_id}', auth=(CWM_MINIO_API_USERNAME, CWM_MINIO_API_PASSWORD))
            if res.status_code != 200:
                print(f'Failed to delete instance: {res.status_code} {res.text}')
    for line in (await minio_api.mc_check_output('ls', MINIO_MC_PROFILE, '--json')).splitlines():
        line = json.loads(line)
        if line.get('type') == 'folder' and is_valid_id_for_cleanup(line.get('key', '').rstrip('/')):
            bucket_name = line['key'].rstrip('/')
            print(bucket_name)
            await minio_api.mc_check_call('rb', f'{MINIO_MC_PROFILE}/{bucket_name}', '--force')
    for line in (await minio_api.mc_check_output('admin', 'user', 'list', MINIO_MC_PROFILE, '--json')).splitlines():
        user = json.loads(line)
        if is_valid_id_for_cleanup(user.get('policyName', '').split('_')[0]):
            print(user['accessKey'])
            await minio_api.mc_check_call('admin', 'user', 'remove', MINIO_MC_PROFILE, user['accessKey'])
    for line in (await minio_api.mc_check_output('admin', 'policy', 'list', MINIO_MC_PROFILE, '--json')).splitlines():
        line = json.loads(line)
        policy_name = line.get('policy')
        if is_valid_id_for_cleanup(policy_name.split('_')[0]):
            print(policy_name)
            await minio_api.mc_check_call('admin', 'policy', 'remove', MINIO_MC_PROFILE, policy_name)
    subprocess.check_call([
        'kubectl', 'exec', '-n', 'minio-tenant-main', 'cwm-1', '-c', 'postgres', '--', 'psql', '-c', dedent('''
            DELETE FROM buckets where instance_id like 'cmalti-%' and name like 'cmaltbp%';
            DELETE FROM instances where id like 'cmalti-%';
        ''')
    ], env={**os.environ, 'KUBECONFIG': os.getenv('KUBECONFIG')})
