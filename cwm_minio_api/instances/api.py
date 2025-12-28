import logging
from contextlib import AsyncExitStack

from ..minio import api as minio_api
from .. import db, common, access_keys


async def create(instance_id):
    common.check_instance_id(instance_id)
    async with db.connection_cursor() as (conn, cur):
        async with AsyncExitStack() as stack:
            access_key = await access_keys.get_access_key(exit_stack=stack)
            await cur.execute('''
                INSERT INTO instances (id, blocked, access_key)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
            ''', (instance_id, False, access_key))
            assert await cur.fetchone(), 'Instance already exists'
            secret_key = common.generate_key(40)
            await minio_api.create_user(access_key, secret_key, exit_stack=stack)
            await conn.commit()
            stack.pop_all()
        instance = await get(instance_id, cur=cur)
        return {
            **instance,
            'secret_key': secret_key
        }


async def update(instance_id, blocked=False, reset_access_key=False):
    async with db.connection_cursor() as (conn, cur):
        instance = await get(instance_id, cur=cur)
        if instance is None:
            raise Exception('Instance not found')
        from ..buckets import api as buckets_api
        bucket_names = [b async for b in buckets_api.list_iterator(instance_id, cur=cur)]
        async with AsyncExitStack() as stack:
            await common.async_run_batches([
                buckets_api.update_block(instance_id, bucket_name, blocked=blocked)
                for bucket_name in bucket_names
            ])
            if reset_access_key:
                old_access_key = instance['access_key']
                access_key = await access_keys.get_access_key(exit_stack=stack)
                await cur.execute('''UPDATE instances SET blocked = %s, access_key = %s WHERE id = %s''', (blocked, access_key, instance_id))
                secret_key = common.generate_key(40)
                await minio_api.create_user(access_key, secret_key, exit_stack=stack)
                await common.async_run_batches([
                    buckets_api.update_instance_access_key(bucket_name, old_access_key, access_key)
                    for bucket_name in bucket_names
                ])
                await minio_api.delete_user(old_access_key)
                await access_keys.delete_access_key(old_access_key)
            else:
                await cur.execute('''UPDATE instances SET blocked = %s WHERE id = %s''', (blocked, instance_id))
            await conn.commit()
            stack.pop_all()
        instance = await get(instance_id, cur=cur)
        return {
            **instance,
            **({'secret_key': secret_key} if reset_access_key else {})
        }


async def delete(instance_id):
    async with db.connection_cursor() as (conn, cur):
        instance = await get(instance_id, cur=None)
        if instance is None:
            raise Exception('Instance not found')
        from ..buckets import api as buckets_api
        bucket_names = [b async for b in buckets_api.list_iterator(instance_id, cur=cur)]
        await common.async_run_batches([
            buckets_api.delete(instance_id, bucket_name)
            for bucket_name in bucket_names
        ])
        access_key = instance['access_key']
        if access_key:
            await minio_api.delete_user(access_key)
            await access_keys.delete_access_key(access_key)
        else:
            logging.warning(f'Delete instance {instance_id}: has no access key set, skipping user deletion')
        await cur.execute('''
            DELETE FROM instances
            WHERE id = %s
        ''', (instance_id,))
        await conn.commit()


async def get(instance_id, cur=None):
    async with db.connection_cursor(cur=cur) as (conn, cur):
        await cur.execute('''
            SELECT id, blocked, access_key
            FROM instances
            WHERE id = %s
        ''', (instance_id,))
        row = await cur.fetchone()
        if row:
            await cur.execute('''
                select count(*) as bucket_count from buckets where instance_id = %s
            ''', (instance_id,))
            bucket_count = (await cur.fetchone())['bucket_count']
            return {
                'instance_id': row['id'],
                'blocked': row['blocked'],
                'num_buckets': bucket_count,
                'access_key': row['access_key']
            }
        else:
            return None


async def list_iterator():
    async with db.connection_cursor() as (conn, cur):
        await cur.execute('SELECT id FROM instances')
        async for row in cur:
            yield row['id']
