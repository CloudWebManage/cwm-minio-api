import time
from contextlib import asynccontextmanager

from psycopg import AsyncConnection
from psycopg.rows import dict_row


from . import config
from .metrics.prometheus import DB_CONN_ACQUIRE_TIME, DB_CONNS_TOTAL


@asynccontextmanager
async def connection_cursor(cur=None):
    if cur is None:
        start_time = time.perf_counter()
        try:
            conn = await AsyncConnection.connect(conninfo=config.DB_CONNSTRING, connect_timeout=3)
            try:
                async with conn.cursor(row_factory=dict_row) as cur:
                    DB_CONNS_TOTAL.labels(outcome="success").inc()
                    DB_CONN_ACQUIRE_TIME.observe(time.perf_counter() - start_time)
                    yield conn, cur
            finally:
                await conn.close()
        except Exception:
            DB_CONNS_TOTAL.labels(outcome="exception").inc()
            DB_CONN_ACQUIRE_TIME.observe(time.perf_counter() - start_time)
            raise
    else:
        yield cur.connection, cur
