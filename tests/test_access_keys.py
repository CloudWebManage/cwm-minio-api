import pytest
from contextlib import AsyncExitStack

from cwm_minio_api import access_keys, db


async def test_stack_failure(cwm_test_db):
    with pytest.raises(Exception):
        async with AsyncExitStack() as exit_stack:
            access_key = await access_keys.get_access_key(exit_stack=exit_stack)
            raise Exception()
    async with db.connection_cursor() as (conn, cur):
        await cur.execute('''
            SELECT 1 FROM access_keys
            WHERE access_key = %s
        ''', (access_key,))
        assert await cur.fetchone() is None
