import json
import random
import time
import logging
import subprocess

from redis import Redis
import requests

from . import config
from ..config import MINIO_MC_PROFILE, MINIO_MC_BINARY


class SharedState:
    @classmethod
    def get_singleton(cls):
        if not hasattr(cls, '_singleton'):
            cls._singleton = cls()
        return cls._singleton

    def __init__(self):
        self._redis = None
        self.debug_enabled = config.CWM_LOAD_TESTS_DEBUG
        self.key_prefix = 'cwm-minio-api:load-tests'
        self.instances = {}
        self.instance_buckets = {}
        self.instance_bucket_files = {}
        self.last_redis_update_ts = None
        self.updating_from_redis = False
        self.disable_update_from_redis = False
        self.tenant_info = None
        self.init_from_json_file = None
        if config.CWM_INIT_FROM_JSON_FILE and not config.CWM_INIT_FROM_JSON_FILE_ONLY_INSTANCE_BUCKETS:
            self.init_from_json_file = config.CWM_INIT_FROM_JSON_FILE

    @property
    def redis(self):
        if not self._redis:
            self._redis = Redis(config.SHARED_STATE_REDIS_HOST, config.SHARED_STATE_REDIS_PORT)
        return self._redis

    def get_tenant_info(self):
        if self.init_from_json_file:
            self.update_from_redis()
            return self.tenant_info
        else:
            return None

    def export(self, filename):
        self.last_redis_update_ts = None
        self.updating_from_redis = False
        config.CWM_INIT_FROM_REDIS = False
        config.CWM_INIT_FROM_JSON_FILE = None
        self.update_from_redis()
        with open(filename, 'w') as f:
            json.dump({
                'instances': self.instances,
                'instance_buckets': self.instance_buckets,
                'instance_bucket_files': self.instance_bucket_files,
                'tenant_info': requests.get(f'https://{config.CWM_MINIO_API_HOST}/tenant/info', auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),).json()
            }, f)

    def debug(self, *args, **kwargs):
        if self.debug_enabled:
            logging.info(*args, **kwargs)

    def clear(self):
        if not config.CWM_KEEP_REDIS_DATA:
            self.debug('Clearing shared state in Redis...')
            self.redis.flushdb()
            self.debug('Shared state cleared.')

    def get_timestamp(self):
        return int(time.time())

    def counter_incr(self, name):
        self.redis.incr(f'{self.key_prefix}:counters:{name}')

    def counter_get(self, name):
        return int(self.redis.get(f'{self.key_prefix}:counters:{name}') or 0)

    def seconds_since(self, past_timestamp, now=None):
        if not now:
            now = self.get_timestamp()
        return int(now) - int(past_timestamp)

    # this method runs once from the master locustfile and initializes this mode of operation
    def init_from_json_file_only_instance_buckets(self):
        self.debug('Initializing shared state from JSON file (only instances and buckets, files will be updated from MinIO)...')
        self.redis.flushdb()
        self.update_from_file(config.CWM_INIT_FROM_JSON_FILE)
        self.instance_bucket_files = {}
        num_instances = 0
        num_buckets = 0
        num_files = 0
        for instance_id, instance in self.instances.items():
            num_instances += 1
            self.add_instance(instance_id, *instance)
            for suffix, buckets in self.instance_buckets.get(instance_id, {}).items():
                for bucket_name, bucket in buckets.items():
                    num_buckets += 1
                    self.upsert_bucket(instance_id, bucket_name, bucket)
                    for line in subprocess.check_output([
                        MINIO_MC_BINARY, "ls", f'{MINIO_MC_PROFILE}/{bucket_name}/', '--json', '--no-color'
                    ]).splitlines():
                        line = json.loads(line)
                        if int(line['size']) in config.CWM_UPDOWNDEL_CONTENT_LENGTH_VALUES:
                            num_files += 1
                            self.add_file(instance_id, bucket_name, line['key'], line['size'])
        self.debug(f'Initialization from JSON file complete, added {num_instances} instances, {num_buckets} buckets and {num_files} files to shared state.')

    def update_from_file(self, filename):
        with open(filename, 'r') as f:
            data = json.load(f)
            self.instances = data.get('instances', {})
            self.instance_buckets = data.get('instance_buckets', {})
            self.instance_bucket_files = data.get('instance_bucket_files', {})
            self.tenant_info = data.get('tenant_info')

    def update_from_redis(self):
        if not self.disable_update_from_redis:
            if config.CWM_INIT_FROM_REDIS:
                self.disable_update_from_redis = True
                should_update = True
            elif self.init_from_json_file:
                should_update = False
                self.disable_update_from_redis = True
                self.update_from_file(self.init_from_json_file)
            else:
                ttl_seconds = 5 if len(self.instances) < 1 else random.randint(240,360)
                should_update = not self.updating_from_redis and (self.last_redis_update_ts is None or self.seconds_since(self.last_redis_update_ts) > ttl_seconds)
            if should_update:
                self.updating_from_redis = True
                try:
                    self.debug('Updating shared state from Redis...')
                    instance_ids = list(self.redis.smembers(f'{self.key_prefix}:instances'))
                    self.debug(f'Found {len(instance_ids)} instances in Redis')
                    for instance_id in instance_ids:
                        instance_id = instance_id.decode('utf-8')
                        access, secret, ts = self.redis.get(f'{self.key_prefix}:instances:{instance_id}').decode('utf-8').split(':')
                        self.instances[instance_id] = access, secret, ts
                        for suffix in ['public', 'private']:
                            buckets_key = f'{self.key_prefix}:instances:{instance_id}:buckets:{suffix}'
                            bucket_names = list(self.redis.smembers(buckets_key))
                            self.debug(f'Found {len(bucket_names)} {suffix} buckets for instance {instance_id} in Redis')
                            for bucket_name in bucket_names:
                                bucket_name = bucket_name.decode('utf-8')
                                bucket_data = self.redis.get(f'{buckets_key}:{bucket_name}')
                                if bucket_data:
                                    bucket = json.loads(bucket_data)
                                    self.instance_buckets.setdefault(instance_id, {}).setdefault(suffix, {})[bucket_name] = bucket
                                    files_key = f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files'
                                    filenames = list(self.redis.smembers(files_key))
                                    self.debug(f'Found {len(filenames)} files for bucket {bucket_name} of instance {instance_id} in Redis')
                                    for filename in filenames:
                                        filename = filename.decode('utf-8')
                                        file_data = self.redis.get(f'{files_key}:{filename}')
                                        if file_data:
                                            content_length, ts = file_data.decode('utf-8').split(':')
                                            self.instance_bucket_files.setdefault(instance_id, {}).setdefault(bucket_name, {})[filename] = content_length, ts
                    self.last_redis_update_ts = self.get_timestamp()
                    self.debug(f'shared state update from Redis complete, last_redis_update_ts={self.last_redis_update_ts}')
                finally:
                    self.updating_from_redis = False

    def add_instance(self, instance_id, instance_access_key, instance_secret_key, now=None):
        now = now or self.get_timestamp()
        self.redis.set(f'{self.key_prefix}:instances:{instance_id}', f'{instance_access_key}:{instance_secret_key}:{now}')
        self.redis.sadd(f'{self.key_prefix}:instances', instance_id)
        self.instances[instance_id] = instance_access_key, instance_secret_key, now

    def delete_instance(self, instance_id):
        self.redis.srem(f'{self.key_prefix}:instances', instance_id)
        self.redis.delete(f'{self.key_prefix}:instances:{instance_id}')
        if instance_id in self.instances:
            del self.instances[instance_id]

    def upsert_bucket(self, instance_id, bucket_name, bucket):
        key = f'{self.key_prefix}:instances:{instance_id}:buckets:'
        key_suffix = 'public' if bucket["public"] else 'private'
        key += key_suffix
        bucket['__ts'] = bucket.get('__ts') or self.get_timestamp()
        self.redis.set(f'{key}:{bucket_name}', json.dumps(bucket))
        self.redis.sadd(key, bucket_name)
        self.instance_buckets.setdefault(instance_id, {}).setdefault(key_suffix, {})[bucket_name] = bucket

    def delete_bucket(self, instance_id, bucket_name):
        for suffix in ['public', 'private']:
            key = f'{self.key_prefix}:instances:{instance_id}:buckets:{suffix}'
            self.redis.srem(key, bucket_name)
            self.redis.delete(f'{key}:{bucket_name}')
            if instance_id in self.instance_buckets:
                if suffix in self.instance_buckets[instance_id]:
                    if bucket_name in self.instance_buckets[instance_id][suffix]:
                        del self.instance_buckets[instance_id][suffix][bucket_name]

    def add_file(self, instance_id, bucket_name, filename, content_length):
        now = self.get_timestamp()
        self.redis.set(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files:{filename}', f'{content_length}:{now}')
        self.redis.sadd(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files', filename)
        self.instance_bucket_files.setdefault(instance_id, {}).setdefault(bucket_name, {})[filename] = content_length, now

    def delete_file(self, instance_id, bucket_name, filename):
        self.redis.srem(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files', filename)
        self.redis.delete(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files:{filename}')
        if instance_id in self.instance_bucket_files:
            if bucket_name in self.instance_bucket_files[instance_id]:
                if filename in self.instance_bucket_files[instance_id][bucket_name]:
                    del self.instance_bucket_files[instance_id][bucket_name][filename]

    def get_bucket_names(self, instance_id, public, ttl_seconds=30):
        self.update_from_redis()
        suffix = 'public' if public else 'private'
        res = []
        for bucket_name, bucket in self.instance_buckets.get(instance_id, {}).get(suffix, {}).items():
            if ttl_seconds:
                if self.seconds_since(bucket['__ts']) > ttl_seconds:
                    res.append(bucket_name)
            else:
                res.append(bucket_name)
        return res

    def get_instance_ids(self, ttl_seconds=30):
        self.update_from_redis()
        res = []
        for instance_id, instance in self.instances.items():
            if ttl_seconds:
                if self.seconds_since(instance[2]) > ttl_seconds:
                    res.append(instance_id)
            else:
                res.append(instance_id)
        return res

    def get_instance(self, instance_id):
        self.update_from_redis()
        instance = self.instances.get(instance_id)
        if instance:
            return instance[0], instance[1]
        else:
            return None

    def get_filenames(self, instance_id, bucket_name, ttl_seconds=30):
        self.update_from_redis()
        res = []
        for filename, filedata in self.instance_bucket_files.get(instance_id, {}).get(bucket_name, {}).items():
            if ttl_seconds:
                if self.seconds_since(filedata[1]) > ttl_seconds:
                    res.append(filename)
            else:
                res.append(filename)
        return res

    def get_filename_content_length(self, instance_id, bucket_name, filename):
        self.update_from_redis()
        filedata = self.instance_bucket_files.get(instance_id, {}).get(bucket_name, {}).get(filename)
        if filedata:
            return filedata[0]
        else:
            return None

    def is_filename_exists(self, instance_id, bucket_name, filename):
        self.update_from_redis()
        return filename in self.instance_bucket_files.get(instance_id, {}).get(bucket_name, {})
