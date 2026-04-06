from contextlib import AsyncExitStack

from .. import access_keys, common, db
from ..instances.api import get as get_instance
from ..minio import api as minio_api


async def create(instance_id):
    async with db.connection_cursor() as (conn, cur):
        instance = await get_instance(instance_id, cur=cur)
        if instance is None:
            raise Exception('Instance not found')
        async with AsyncExitStack() as stack:
            access_key = await access_keys.get_access_key(exit_stack=stack)
            await cur.execute('''
                INSERT INTO credentials (instance_id, access_key)
                VALUES (%s, %s)
            ''', (instance_id, access_key))
            secret_key = common.generate_key(40)
            await minio_api.create_user(access_key, secret_key, exit_stack=stack)
            await conn.commit()
            stack.pop_all()
        return {
            'access_key': access_key,
            'secret_key': secret_key,
        }


async def get(access_key, cur=None):
    async with db.connection_cursor(cur=cur) as (conn, cur):
        await cur.execute('''
            SELECT instance_id, access_key
            FROM credentials
            WHERE access_key = %s
        ''', (access_key,))
        row = await cur.fetchone()
        if row is None:
            return None
        return {
            'instance_id': row['instance_id'],
            'access_key': row['access_key'],
        }


async def delete(access_key):
    async with db.connection_cursor() as (conn, cur):
        credential = await get(access_key, cur=cur)
        if credential is None:
            raise Exception('Credentials not found')
        await cur.execute('''
            SELECT 1
            FROM bucket_credentials
            WHERE access_key = %s
        ''', (access_key,))
        if await cur.fetchone() is not None:
            raise Exception('Credentials are assigned to buckets')
        await cur.execute('''
            DELETE FROM credentials
            WHERE access_key = %s
        ''', (access_key,))
        await cur.execute('''
            DELETE FROM access_keys
            WHERE access_key = %s
        ''', (access_key,))
        await minio_api.delete_user(access_key)
        await conn.commit()


async def list_iterator(instance_id, cur=None):
    async with db.connection_cursor(cur=cur) as (conn, cur):
        await cur.execute('''
            SELECT access_key
            FROM credentials
            WHERE instance_id = %s
            ORDER BY access_key
        ''', (instance_id,))
        async for row in cur:
            yield {
                'access_key': row['access_key'],
            }
