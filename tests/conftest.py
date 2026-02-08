import os
import sys
import logging
from uuid import uuid1

import pytest

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
    from cwm_minio_api.minio.api import mc_check_call as _mc_check_call, mc_check_output as _mc_check_output
    tracker = []

    state = {
        "tracker": tracker
    }

    async def mc_check_call(*args):
        nargs = []
        for arg in args:
            if arg.startswith('/'):
                with open(arg) as f:
                    content = f.read()
                arg = f'FILE::{content}'
            nargs.append(arg)
        tracker.append(('mc_check_call', tuple(nargs)))
        if 'intercept' in state:
            return await state['intercept'](_mc_check_call, "mc_check_call", *args)
        else:
            return await _mc_check_call(*args)

    async def mc_check_output(*args):
        tracker.append(('mc_check_output', args))
        if 'intercept' in state:
            return await state['intercept'](_mc_check_output, "mc_check_output", *args)
        else:
            return await _mc_check_output(*args)

    monkeypatch.setattr('cwm_minio_api.minio.api.mc_check_call', mc_check_call)
    monkeypatch.setattr('cwm_minio_api.minio.api.mc_check_output', mc_check_output)

    def tracker_get_calls():
        l = []
        for item in state["tracker"]:
            mname = item[0]
            margs = item[1]
            l.append((mname, margs))
        state["tracker"].clear()
        return l

    def tracker_assert_calls(expected_calls):
        actual_calls = tracker_get_calls()
        assert len(actual_calls) == len(expected_calls), f'Expected {len(expected_calls)} calls, got {len(actual_calls)} calls'
        for actual, expected in zip(actual_calls, expected_calls):
            actual_method_name, expected_method_name = actual[0], expected[0]
            actual_args, expected_args = actual[1], expected[1:]
            assert actual_method_name == expected_method_name, f'Expected method {expected_method_name}, got {actual_method_name}'
            if len(actual_args) == len(expected_args):
                assert actual_args == expected_args, f'Arguments do not match for call {actual_method_name}: expected {expected_args}, got {actual_args}'
            else:
                for a, e in zip(actual_args, expected_args):
                    assert a == e, f'Arguments do not match for call {actual_method_name}: expected {expected_args}, got {actual_args}'
        return actual_calls

    state['tracker_get_calls'] = tracker_get_calls
    state['tracker_assert_calls'] = tracker_assert_calls
    yield state


@pytest.fixture(scope='function')
async def cwm_test_minio(monkeypatch, test_db):
    test_prefix = f'cwmtest-{uuid1().hex[:10]}'
    monkeypatch.setattr('cwm_minio_api.config.MINIO_MC_PROFILE', 'cwmtest')
    await async_subprocess_check_call(MINIO_MC_BINARY, 'alias', 'set', 'cwmtest', 'http://localhost:9000', 'cwm', '12345678')
    print(f'Using MinIO test prefix: {test_prefix}')
    yield 'cwmtest', test_prefix
