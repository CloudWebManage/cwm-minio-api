from .. import db


async def create(instance_id, max_size_gb):
    async with db.connection_cursor() as (conn, cur):
        if await get(instance_id, cur=cur) is not None:
            raise Exception('Instance already exists')
        await cur.execute('''
            INSERT INTO instances (id, max_size_gb)
            VALUES (%s, %s)
        ''', (instance_id, max_size_gb))
        await conn.commit()
        return await get(instance_id, cur=cur)


async def update(instance_id, max_size_gb):
    async with db.connection_cursor() as (conn, cur):
        instance = await get(instance_id, cur=cur)
        if instance is None:
            raise Exception('Instance not found')
        if instance['num_buckets'] > 0:
            raise Exception('Instance has buckets, cannot update')
        await cur.execute('''
            UPDATE instances
            SET max_size_gb = %s
            WHERE id = %s
        ''', (max_size_gb, instance_id))
        await conn.commit()
        return await get(instance_id, cur=cur)


async def delete(instance_id):
    async with db.connection_cursor() as (conn, cur):
        instance = await get(instance_id, cur=None)
        if instance is None:
            raise Exception('Instance not found')
        if instance['num_buckets'] > 0:
            raise Exception('Instance has buckets, cannot delete')
        await cur.execute('''
            DELETE FROM instances
            WHERE id = %s
        ''', (instance_id,))
        await conn.commit()


async def get(instance_id, cur=None):
    async with db.connection_cursor(cur=cur) as (conn, cur):
        await cur.execute('''
            SELECT id, max_size_gb
            FROM instances
            WHERE id = %s
        ''', (instance_id,))
        row = await cur.fetchone()
        if row:
            await cur.execute('''
                select count(*) as bucket_count from buckets where instance_id = %s
            ''', (instance_id,))
            bucket_count = cur.fetchone()['bucket_count']
            return {
                'status': 'active',
                'instance_id': row['id'],
                'max_size_gb': row['max_size_gb'],
                'num_buckets': bucket_count
            }
        else:
            return None


async def list_iterator():
    async with db.connection_cursor() as (conn, cur):
        await cur.execute('SELECT id FROM instances')
        async for row in cur:
            yield row['id']
