import os

import dotenv


_dotenvpath = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
if os.path.exists(_dotenvpath):
    dotenv.load_dotenv(_dotenvpath)


CWM_LOAD_TESTS_DEBUG = os.getenv("CWM_LOAD_TESTS_DEBUG", "no").lower() == "yes"

CWM_MINIO_API_HOST = os.getenv("CWM_MINIO_API_HOST")
CWM_MINIO_API_USERNAME = os.getenv("CWM_MINIO_API_USERNAME")
CWM_MINIO_API_PASSWORD = os.getenv("CWM_MINIO_API_PASSWORD")

# specifies if to initialize shared state from a JSON file or from the existing data in Redis.
# if none of these are set - shared state will be cleared at the start of the test.
CWM_INIT_FROM_JSON_FILE = os.getenv("CWM_INIT_FROM_JSON_FILE")
CWM_INIT_FROM_REDIS = os.getenv("CWM_INIT_FROM_REDIS", "").lower() == "yes"

# if set to yes - will only get instance and buckets from the json file
# the other information will be updated from the actual MinIO instance on locustfile start and then stored in redis
# from that point on the test will run as if initialized from redis
CWM_INIT_FROM_JSON_FILE_ONLY_INSTANCE_BUCKETS = os.getenv("CWM_INIT_FROM_JSON_FILE_ONLY_INSTANCE_BUCKETS", "").lower() == "yes"

# specifies what to do at the end of the tests - whether to keep data or clear it
CWM_KEEP_INSTANCE = os.getenv("CWM_KEEP_INSTANCE", "").lower() == "yes"
CWM_KEEP_BUCKETS = os.getenv("CWM_KEEP_BUCKETS", "").lower() == "yes"
CWM_KEEP_REDIS_DATA = os.getenv("CWM_KEEP_REDIS_DATA", "").lower() == "yes"


CWM_UPDOWNDEL_ENABLED = os.getenv("CWM_UPDOWNDEL_ENABLED", "yes").lower() == "yes"
# waits for all updowndel users to finish on_start and then stops them and starts getgetter users
# this only works if: there is more than one user type enabled, including updowndel, and updowndel has fixed_count
CWM_UPDOWNDEL_SEPARATE_FROM_OTHER_USERS = os.getenv("CWM_UPDOWNDEL_SEPARATE_FROM_OTHER_USERS", "no").lower() == "yes"
CWM_UPDOWNDEL_USER_WEIGHT = int(os.getenv("CWM_UPDOWNDEL_USER_WEIGHT", "1"))
CWM_UPDOWNDEL_FIXED_COUNT = int(os.getenv("CWM_UPDOWNDEL_FIXED_COUNT", "0"))
CWM_UPDOWNDEL_UPLOAD_WEIGHT = int(os.getenv("CWM_UPDOWNDEL_UPLOAD_WEIGHT", "1"))
CWM_UPDOWNDEL_DOWNLOAD_WEIGHT = int(os.getenv("CWM_UPDOWNDEL_DOWNLOAD_WEIGHT", "1"))
CWM_UPDOWNDEL_DELETE_WEIGHT = int(os.getenv("CWM_UPDOWNDEL_DELETE_WEIGHT", "1"))
CWM_UPDOWNDEL_AWS_WEIGHT = int(os.getenv("CWM_UPDOWNDEL_AWS_WEIGHT", "0"))
CWM_UPDOWNDEL_PUBLIC_WEIGHT = int(os.getenv("CWM_UPDOWNDEL_PUBLIC_WEIGHT", "1"))
CWM_UPDOWNDEL_PRIVATE_WEIGHT = int(os.getenv("CWM_UPDOWNDEL_PRIVATE_WEIGHT", "1"))
CWM_UPDOWNDEL_CONTENT_LENGTH_VALUES = os.getenv("CWM_UPDOWNDEL_CONTENT_LENGTH_VALUES", "5,1024,1048576")
CWM_UPDOWNDEL_CONTENT_LENGTH_VALUES = [int(x.strip()) for x in CWM_UPDOWNDEL_CONTENT_LENGTH_VALUES.split(",") if x.strip()]
CWM_UPDOWNDEL_CONTENT_LENGTH_WEIGHTS = os.getenv("CWM_UPDOWNDEL_CONTENT_LENGTH_WEIGHTS", "100,10,1")
CWM_UPDOWNDEL_CONTENT_LENGTH_WEIGHTS = [int(x.strip()) for x in CWM_UPDOWNDEL_CONTENT_LENGTH_WEIGHTS.split(",") if x.strip()]
CWM_UPDOWNDEL_MAX_FILES_PER_BUCKET = int(os.getenv("CWM_UPDOWNDEL_MAX_FILES_PER_BUCKET", "10"))
CWM_UPDOWNDEL_MIN_FILES_PER_BUCKET = int(os.getenv("CWM_UPDOWNDEL_MIN_FILES_PER_BUCKET", "5"))
CWM_UPDOWNDEL_CONCURRENCY = int(os.getenv("CWM_UPDOWNDEL_CONCURRENCY", "10"))
# will create these many buckets and files on start before starting the load test tasks
CWM_UPDOWNDEL_ON_START_NUM_BUCKETS = int(os.getenv("CWM_UPDOWNDEL_ON_START_NUM_BUCKETS", "2"))
CWM_UPDOWNDEL_ON_START_NUM_FILES_PER_BUCKET = int(os.getenv("CWM_UPDOWNDEL_ON_START_NUM_FILES_PER_BUCKET", "3"))
# if set to no - instances and buckets will not be created, will only use existing ones from shared state
CWM_UPDOWNDEL_CREATE_INSTANCE_BUCKETS = os.getenv("CWM_UPDOWNDEL_CREATE_INSTANCE_BUCKETS", "yes").lower() == "yes"

CWM_GETGETTER_ENABLED = os.getenv("CWM_GETGETTER_ENABLED", "yes").lower() == "yes"
CWM_GETGETTER_USER_WEIGHT = int(os.getenv("CWM_GETGETTER_USER_WEIGHT", "1"))
CWM_GETGETTER_FIXED_COUNT = int(os.getenv("CWM_GETGETTER_FIXED_COUNT", "0"))
CWM_GETGETTER_CONCURRENCY = int(os.getenv("CWM_GETGETTER_CONCURRENCY", "10"))

SHARED_STATE_REDIS_HOST = os.getenv("SHARED_STATE_REDIS_HOST", "localhost")
SHARED_STATE_REDIS_PORT = int(os.getenv("SHARED_STATE_REDIS_PORT", "6379"))

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

