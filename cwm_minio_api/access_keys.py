from . import db, common, config


async def get_access_key(exit_stack=None):
    for try_num in range(20):
        async with db.connection_cursor() as (conn, cur):
            access_key = common.generate_key(config.ACCESS_KEY_LENGTH)
            await cur.execute('''
                insert into access_keys (access_key) values (%s)
                ON CONFLICT DO NOTHING
                RETURNING access_key
            ''', (access_key,))
            if await cur.fetchone():
                if exit_stack:
                    exit_stack.push_async_callback(delete_access_key, access_key)
                return access_key
    raise Exception('Failed to get unique access key after 20 tries')


async def delete_access_key(access_key):
    async with db.connection_cursor() as (conn, cur):
        await cur.execute('''
            DELETE FROM access_keys
            WHERE access_key = %s
        ''', (access_key,))
        await conn.commit()
