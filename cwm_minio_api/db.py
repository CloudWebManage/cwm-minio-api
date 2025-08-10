from contextlib import asynccontextmanager

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from . import config


pool = AsyncConnectionPool(
    conninfo=config.DB_CONNSTRING,
    check=AsyncConnectionPool.check_connection,
    **{
        'open': False,
        **config.DB_POOL_KWARGS
    }
)


@asynccontextmanager
async def connection_cursor(cur=None):
    if cur is None:
        if pool.closed:
            await pool.open()
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                yield conn, cur
    else:
        yield cur.connection, cur
