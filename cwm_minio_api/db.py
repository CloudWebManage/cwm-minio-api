import time
from contextlib import asynccontextmanager

import psycopg_pool
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from . import config, common
from .metrics.prometheus import DB_CONN_ACQUIRE_TIME, DB_CONNS_TOTAL


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
        start_time = time.perf_counter()
        try:
            if pool.closed:
                await pool.open()
            async with pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    DB_CONNS_TOTAL.labels(outcome="success").inc()
                    DB_CONN_ACQUIRE_TIME.observe(time.perf_counter() - start_time)
                    yield conn, cur
        except psycopg_pool.PoolTimeout:
            DB_CONNS_TOTAL.labels(outcome="pool_timeout").inc()
            DB_CONN_ACQUIRE_TIME.observe(time.perf_counter() - start_time)
            raise common.ServerOverloadedException("Database connection pool exhausted")
        except Exception:
            DB_CONNS_TOTAL.labels(outcome="exception").inc()
            DB_CONN_ACQUIRE_TIME.observe(time.perf_counter() - start_time)
            raise
    else:
        yield cur.connection, cur
