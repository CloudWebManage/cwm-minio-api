import os
import sys
import logging
from uuid import uuid1

import pytest

from cwm_minio_api.db import get_async_connection_pool
from cwm_minio_api.common import async_subprocess_check_call
from cwm_minio_api.config import MINIO_MC_BINARY


logging.basicConfig(
    level='DEBUG',
    handlers=[logging.StreamHandler(sys.stderr)]
)


@pytest.fixture(scope='function')
async def test_db(monkeypatch):
    db_name = f'cwm_test_{uuid1().hex}'
    monkeypatch.setattr('cwm_minio_api.config.DB_CONNSTRING', f'postgresql://postgres:123456@localhost/{db_name}')
    monkeypatch.setattr('cwm_minio_api.db.pool', get_async_connection_pool())
    await async_subprocess_check_call('docker', 'compose', 'exec', '--user', 'postgres', 'db', 'createdb', db_name)
    try:
        await async_subprocess_check_call('bin/migrate.sh', 'up', env={
            **os.environ,
            'MIGRATE_DATABASE_URL': f'postgres://postgres:123456@localhost:5432/{db_name}?sslmode=disable'
        })
        yield
    finally:
        await async_subprocess_check_call('docker', 'compose', 'exec', '--user', 'postgres', 'db', 'dropdb', '--force', db_name)


@pytest.fixture(scope='function')
async def cwm_test_db(monkeypatch, test_db):
    monkeypatch.setattr('cwm_minio_api.config.MINIO_MC_BINARY', 'true')


@pytest.fixture(scope='function')
async def cwm_test_minio(monkeypatch, test_db):
    test_prefix = f'cwmtest-{uuid1().hex[:10]}'
    monkeypatch.setattr('cwm_minio_api.config.MINIO_MC_PROFILE', 'cwmtest')
    await async_subprocess_check_call(MINIO_MC_BINARY, 'alias', 'set', 'cwmtest', 'http://localhost:9000', 'cwm', '12345678')
    print(f'Using MinIO test prefix: {test_prefix}')
    yield 'cwmtest', test_prefix
