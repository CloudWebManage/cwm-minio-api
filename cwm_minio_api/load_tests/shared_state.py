import json
import random
import time

from redis import Redis

from . import config


class SharedState:
    def __init__(self):
        self.redis = Redis(config.SHARED_STATE_REDIS_HOST, config.SHARED_STATE_REDIS_PORT)
        self.debug_enabled = config.CWM_LOAD_TESTS_DEBUG
        self.key_prefix = 'cwm-minio-api:load-tests'
        self.instances = {}
        self.instance_buckets = {}
        self.instance_bucket_files = {}
        self.last_redis_update_ts = None
        self.updating_from_redis = False

    def debug(self, *args, **kwargs):
        if self.debug_enabled:
            print(*args, **kwargs)

    def clear(self):
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

    def update_from_redis(self):
        ttl_seconds = 5 if len(self.instances) < 1 else random.randint(240,360)
        if not self.updating_from_redis and (self.last_redis_update_ts is None or self.seconds_since(self.last_redis_update_ts) > ttl_seconds):
            self.updating_from_redis = True
            try:
                self.debug('Updating shared state from Redis...')
                for instance_id in self.redis.smembers(f'{self.key_prefix}:instances'):
                    instance_id = instance_id.decode('utf-8')
                    access, secret, ts = self.redis.get(f'{self.key_prefix}:instances:{instance_id}').decode('utf-8').split(':')
                    self.instances[instance_id] = access, secret, ts
                    for suffix in ['public', 'private']:
                        buckets_key = f'{self.key_prefix}:instances:{instance_id}:buckets:{suffix}'
                        for bucket_name in self.redis.smembers(buckets_key):
                            bucket_name = bucket_name.decode('utf-8')
                            bucket_data = self.redis.get(f'{buckets_key}:{bucket_name}')
                            if bucket_data:
                                bucket = json.loads(bucket_data)
                                self.instance_buckets.setdefault(instance_id, {}).setdefault(suffix, {})[bucket_name] = bucket
                                files_key = f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files'
                                for filename in self.redis.smembers(files_key):
                                    filename = filename.decode('utf-8')
                                    file_data = self.redis.get(f'{files_key}:{filename}')
                                    if file_data:
                                        content_length, ts = file_data.decode('utf-8').split(':')
                                        self.instance_bucket_files.setdefault(instance_id, {}).setdefault(bucket_name, {})[filename] = content_length, ts
                self.last_redis_update_ts = self.get_timestamp()
            finally:
                self.updating_from_redis = False

    def add_instance(self, instance_id, instance_access_key, instance_secret_key):
        now = self.get_timestamp()
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
        bucket['__ts'] = self.get_timestamp()
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

    def is_filename_exists(self, instance_id, bucket_name, filename):
        self.update_from_redis()
        return filename in self.instance_bucket_files.get(instance_id, {}).get(bucket_name, {})
