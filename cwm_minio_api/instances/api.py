from .. import db


async def create(instance_id):
    async with db.connection_cursor() as (conn, cur):
        if await get(instance_id, cur=cur) is not None:
            raise Exception('Instance already exists')
        await cur.execute('''
            INSERT INTO instances (id, blocked)
            VALUES (%s, %s)
        ''', (instance_id, False))
        await conn.commit()
        return await get(instance_id, cur=cur)


async def update(instance_id, blocked=None):
    async with db.connection_cursor() as (conn, cur):
        instance = await get(instance_id, cur=cur)
        if instance is None:
            raise Exception('Instance not found')
        blocked = blocked if blocked is not None else instance['blocked']
        from ..buckets import api as buckets_api
        async for bucket_name in buckets_api.list_iterator(instance_id, cur=cur):
            await buckets_api.update(instance_id, bucket_name, blocked=blocked)
        await cur.execute('''
            UPDATE instances
            SET blocked = %s
            WHERE id = %s
        ''', (blocked, instance_id))
        await conn.commit()
        return await get(instance_id, cur=cur)


async def delete(instance_id):
    async with db.connection_cursor() as (conn, cur):
        instance = await get(instance_id, cur=None)
        if instance is None:
            raise Exception('Instance not found')
        from ..buckets import api as buckets_api
        async for bucket_name in buckets_api.list_iterator(instance_id, cur=cur):
            await buckets_api.delete(instance_id, bucket_name)
        await cur.execute('''
            DELETE FROM instances
            WHERE id = %s
        ''', (instance_id,))
        await conn.commit()


async def get(instance_id, cur=None):
    async with db.connection_cursor(cur=cur) as (conn, cur):
        await cur.execute('''
            SELECT id, blocked
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
                'num_buckets': bucket_count
            }
        else:
            return None


async def list_iterator():
    async with db.connection_cursor() as (conn, cur):
        await cur.execute('SELECT id FROM instances')
        async for row in cur:
            yield row['id']
