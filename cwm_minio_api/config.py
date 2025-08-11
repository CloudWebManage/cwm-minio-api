import os

import dotenv
import orjson


dotenv.load_dotenv()


CWM_LOG_LEVEL = os.getenv("CWM_LOG_LEVEL", "DEBUG")
CWM_ENV_TYPE = os.getenv("CWM_ENV_TYPE")

# https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
DB_CONNSTRING = os.getenv('DB_CONNSTRING', 'postgresql://postgres:123456@localhost/postgres')
# https://www.psycopg.org/psycopg3/docs/api/pool.html#the-connectionpool-class
DB_POOL_KWARGS = orjson.loads(os.getenv('DB_POOL_KWARGS_JSON', '{}'))

MINIO_MC_BINARY = os.getenv('MINIO_MC_BINARY', 'mc')
MINIO_MC_PROFILE = os.getenv('MINIO_MC_PROFILE', 'cwm')

TENANT_INFO = orjson.loads(os.getenv('TENANT_INFO_JSON', '{}'))
