import os
from uuid import uuid1

import pytest

from cwm_minio_api.db import get_async_connection_pool
from cwm_minio_api.common import async_subprocess_check_call


@pytest.fixture(scope='function')
async def cwm_test_db(monkeypatch):
    db_name = f'cwm_test_{uuid1().hex}'
    monkeypatch.setattr('cwm_minio_api.config.DB_CONNSTRING', f'postgresql://postgres:123456@localhost/{db_name}')
    monkeypatch.setattr('cwm_minio_api.config.MINIO_MC_BINARY', 'true')
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
