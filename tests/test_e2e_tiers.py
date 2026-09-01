import asyncio
import json
import os
import time
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from uuid import uuid4
import base64

import dotenv
import pytest
import requests

from cwm_minio_api.common import async_subprocess_check_call, async_subprocess_check_output
from cwm_minio_api.config import MINIO_MC_BINARY


dotenv.load_dotenv()


CWM_MINIO_API_URL = os.getenv('CWM_MINIO_API_URL')
CWM_MINIO_API_USERNAME = os.getenv('CWM_MINIO_API_USERNAME')
CWM_MINIO_API_PASSWORD = os.getenv('CWM_MINIO_API_PASSWORD')
WAIT_TIMEOUT_SECONDS = 12 * 60 * 60
POLL_INTERVAL_SECONDS = 60


async def cwm_minio_api(path, method='get', **kwargs):
    res = requests.request(
        method,
        os.path.join(CWM_MINIO_API_URL, path),
        auth=(CWM_MINIO_API_USERNAME, CWM_MINIO_API_PASSWORD),
        **kwargs,
    )
    if res.status_code != 200:
        raise Exception(f'Error calling CWM MinIO API: {res.status_code} {res.text}')
    return res.json()


async def object_stat(target):
    output = await async_subprocess_check_output(MINIO_MC_BINARY, 'stat', target, '--json')
    return json.loads(output)


async def wait_for_stat(target, description, condition):
    print("wait for stat", description)
    started = time.monotonic()
    next_progress_report = started
    while True:
        stat = await object_stat(target)
        if condition(stat):
            elapsed = time.monotonic() - started
            print(f'{description} after {elapsed / 3600:.2f} hours')
            return stat

        now = time.monotonic()
        elapsed = now - started
        if elapsed >= WAIT_TIMEOUT_SECONDS:
            raise TimeoutError(f'Timed out after 12 hours waiting for {description}: {stat}')
        if now >= next_progress_report:
            print(f'Waiting for {description}; elapsed {elapsed / 3600:.2f} hours')
            next_progress_report = now + 10 * 60
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


def is_restored(stat):
    restore = stat.get('restore', {})
    expiry_time = restore.get('ExpiryTime')
    if restore.get('OngoingRestore') is not False or not expiry_time:
        return False
    expiry = datetime.fromisoformat(expiry_time.replace('Z', '+00:00'))
    return expiry >= datetime.now(timezone.utc) + timedelta(days=1)


@pytest.mark.skipif(os.getenv('E2E_TIERS') != 'yes', reason='Tiering E2E test is disabled')
async def test_tier_transitions():
    """Long-running tier test for clusters using 1-hour/3-access low/high thresholds.

    This test can take several hours and requires a tierer configured with
    low_hours=1, low_threshold=3, high_hours=1, and high_threshold=3.
    """
    if not CWM_MINIO_API_URL or not CWM_MINIO_API_USERNAME or not CWM_MINIO_API_PASSWORD:
        pytest.skip('CWM MinIO API environment variables are not set')

    suffix = uuid4().hex[:12]
    instance_id = f'cwm-e2e-tiers-{suffix}'
    bucket_name = f'cwm-e2e-tiers-{suffix}'
    alias = f'cwme2etiers-{suffix}'
    object_name = 'tier-test-object'
    target = f'{alias}/{bucket_name}/{object_name}'

    print("instance_id:", instance_id)
    print("mc alias:", alias)
    print("bucket_name:", bucket_name)
    print("object_name:", object_name)
    print("redis key suffix:", base64.b64encode(bucket_name.encode()).decode().rstrip("=")+":"+base64.b64encode(object_name.encode()).decode().rstrip("="))

    async with AsyncExitStack() as exit_stack:
        instance = await cwm_minio_api(
            'instances/create', method='post', json={'instance_id': instance_id},
        )
        exit_stack.push_async_callback(
            cwm_minio_api, 'instances/delete', method='delete', params={'instance_id': instance_id},
        )
        await cwm_minio_api(
            'buckets/create', method='post',
            json={'instance_id': instance_id, 'bucket_name': bucket_name, 'public': False},
        )
        exit_stack.push_async_callback(
            cwm_minio_api, 'buckets/delete', method='delete',
            params={'instance_id': instance_id, 'bucket_name': bucket_name},
        )

        tenant_info = await cwm_minio_api('tenant/info')
        await async_subprocess_check_call(
            MINIO_MC_BINARY, 'alias', 'set', alias, tenant_info['api_url'],
            instance['access_key'], instance['secret_key'],
        )
        exit_stack.push_async_callback(
            async_subprocess_check_call, MINIO_MC_BINARY, 'alias', 'rm', alias,
        )

        await async_subprocess_check_call(MINIO_MC_BINARY, 'cp', 'README.md', target)
        exit_stack.push_async_callback(
            async_subprocess_check_call, MINIO_MC_BINARY, 'rm', target,
        )

        await wait_for_stat(
            target,
            'object transition to LOW',
            lambda stat: stat.get('metadata', {}).get('X-Amz-Storage-Class') == 'LOW',
        )

        for _ in range(3):
            await async_subprocess_check_output(MINIO_MC_BINARY, 'cat', target)

        await wait_for_stat(target, 'object restore to high tier', is_restored)
