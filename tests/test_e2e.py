import os
import json
from contextlib import AsyncExitStack

import dotenv
import pytest
import requests

from cwm_minio_api.common import async_subprocess_check_call, async_subprocess_check_output, async_subprocess_status_output
from cwm_minio_api.config import MINIO_MC_BINARY

dotenv.load_dotenv()


CWM_MINIO_API_URL = os.getenv("CWM_MINIO_API_URL")
CWM_MINIO_API_USERNAME = os.getenv("CWM_MINIO_API_USERNAME")
CWM_MINIO_API_PASSWORD = os.getenv("CWM_MINIO_API_PASSWORD")


async def cwm_minio_api(path, **params):
    res = requests.get(
        os.path.join(CWM_MINIO_API_URL, path),
        params=params,
        auth=(CWM_MINIO_API_USERNAME, CWM_MINIO_API_PASSWORD),
    )
    if res.status_code != 200:
        raise Exception(f"Error calling CWM MinIO API: {res.status_code} {res.text}")
    return res.json()


def parse_json_lines(output):
    lines = []
    for line in output.splitlines():
        if line.strip():
            lines.append(json.loads(line))
    return lines


@pytest.mark.skipif(os.getenv("E2E") != 'yes', reason="E2E tests are disabled")
async def test():
    if not CWM_MINIO_API_URL or not CWM_MINIO_API_USERNAME or not CWM_MINIO_API_PASSWORD:
        print('Skipping E2E Tests - env vars are not set')
        return
    print(f'Starting E2E Tests on {CWM_MINIO_API_URL}')
    tenant_info = await cwm_minio_api('tenant/info')
    assert tenant_info.keys() == {'api_url', 'console_url', 'prometheus_url'}
    api_url = tenant_info['api_url']
    console_url = tenant_info['console_url']
    prometheus_url = tenant_info['prometheus_url']
    print(f'API URL: {api_url}')
    print(f'Console URL: {console_url}')
    print(f'Prometheus URL: {prometheus_url}')
    instance_id = '__cwm_e2e_test_instance__'
    print(f'Instance ID: {instance_id}')
    async with AsyncExitStack() as exit_stack:
        created_instance = await cwm_minio_api('instances/create', instance_id=instance_id)
        exit_stack.push_async_callback(cwm_minio_api, 'instances/delete', instance_id=instance_id)
        assert created_instance == {
            'instance_id': instance_id,
            'blocked': False,
            'num_buckets': 0,
        }
        assert instance_id in (await cwm_minio_api('instances/list'))
        assert (await cwm_minio_api('instances/get', instance_id=instance_id)) == created_instance
        bucket_name = 'cwm-e2e-test-bucket'
        created_bucket = await cwm_minio_api('buckets/create', instance_id=instance_id, bucket_name=bucket_name, public=False)
        assert created_bucket.keys() == {'access_key', 'blocked', 'bucket_name', 'instance_id', 'public', 'secret_key'}
        assert created_bucket['blocked'] is False
        assert created_bucket['bucket_name'] == bucket_name
        assert created_bucket['instance_id'] == instance_id
        assert created_bucket['public'] is False
        assert created_bucket['access_key'].startswith(f'{bucket_name}:')
        assert len(created_bucket['access_key']) == len(bucket_name) + 11
        assert len(created_bucket['secret_key']) == 40
        await async_subprocess_check_call(MINIO_MC_BINARY, 'alias', 'set', 'cwme2etest', api_url, created_bucket['access_key'], created_bucket['secret_key'])
        exit_stack.push_async_callback(async_subprocess_check_call, MINIO_MC_BINARY, 'alias', 'rm', 'cwme2etest')
        ls_buckets = parse_json_lines(await async_subprocess_check_output(MINIO_MC_BINARY, 'ls', 'cwme2etest', '--json'))
        assert len(ls_buckets) == 1
        ls_bucket = ls_buckets[0]
        assert ls_bucket['type'] == 'folder'
        assert ls_bucket['key'] == f'{bucket_name}/'
        assert ls_bucket['size'] == 0
        await async_subprocess_check_call(MINIO_MC_BINARY, 'cp', 'README.md', f'cwme2etest/{bucket_name}/')
        await async_subprocess_check_call(MINIO_MC_BINARY, 'cp', 'README.md', f'cwme2etest/{bucket_name}/README2.md')
        await async_subprocess_check_call(MINIO_MC_BINARY, 'cp', 'README.md', f'cwme2etest/{bucket_name}/README3.md')
        ls_files = parse_json_lines(await async_subprocess_check_output(MINIO_MC_BINARY, 'ls', f'cwme2etest/{bucket_name}', '--json'))
        assert len(ls_files) == 3
        assert set([f['key'] for f in ls_files]) == {'README.md', 'README2.md', 'README3.md'}
        readme_content = await async_subprocess_check_output(MINIO_MC_BINARY, 'cat', f'cwme2etest/{bucket_name}/README.md')
        with open('README.md', 'r') as f:
            assert readme_content == f.read().strip()
        res = requests.get(os.path.join(api_url, bucket_name, 'README.md'))
        assert res.status_code == 403
        await cwm_minio_api('buckets/update', instance_id=instance_id, bucket_name=bucket_name, public=True, blocked=False)
        res = requests.get(os.path.join(api_url, bucket_name, 'README.md'))
        assert res.status_code == 200
        assert res.text.strip() == readme_content.strip()
        await cwm_minio_api('buckets/update', instance_id=instance_id, bucket_name=bucket_name, public=False, blocked=True)
        status, _ = await async_subprocess_status_output(MINIO_MC_BINARY, 'ls', 'cwme2etest', '--json')
        assert status == 1
