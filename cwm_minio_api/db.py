from contextlib import asynccontextmanager

import psycopg_pool
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from . import config, common


def get_async_connection_pool():
    return AsyncConnectionPool(
        conninfo=config.DB_CONNSTRING,
        check=AsyncConnectionPool.check_connection,
        **{
            'open': False,
            **config.DB_POOL_KWARGS
        }
    )


pool = get_async_connection_pool()


@asynccontextmanager
async def connection_cursor(cur=None):
    if cur is None:
        try:
            if pool.closed:
                await pool.open()
            async with pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    yield conn, cur
        except psycopg_pool.PoolTimeout:
            raise common.ServerOverloadedException("Database connection pool exhausted")
    else:
        yield cur.connection, cur
