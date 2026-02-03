import json

from redis import Redis

from . import config


def clear():
    print('Clearing shared state in Redis...')
    redis = Redis(config.SHARED_STATE_REDIS_HOST, config.SHARED_STATE_REDIS_PORT)
    redis.flushdb()
    print('Shared state cleared.')


class SharedState:
    def __init__(self):
        self.redis = Redis(config.SHARED_STATE_REDIS_HOST, config.SHARED_STATE_REDIS_PORT)
        self.key_prefix = 'cwm-minio-api:load-tests'

    def get_timestamp(self):
        return int(self.redis.time()[0])

    def seconds_since(self, past_timestamp, now=None):
        if not now:
            now = self.get_timestamp()
        return int(now) - int(past_timestamp)

    def add_instance(self, instance_id, instance_access_key, instance_secret_key):
        self.redis.set(f'{self.key_prefix}:instances:{instance_id}', f'{instance_access_key}:{instance_secret_key}:{self.get_timestamp()}')
        self.redis.sadd(f'{self.key_prefix}:instances', instance_id)

    def delete_instance(self, instance_id):
        self.redis.srem(f'{self.key_prefix}:instances', instance_id)
        self.redis.delete(f'{self.key_prefix}:instances:{instance_id}')

    def upsert_bucket(self, instance_id, bucket_name, bucket):
        key = f'{self.key_prefix}:instances:{instance_id}:buckets:'
        key += 'public' if bucket["public"] else 'private'
        bucket['__ts'] = self.get_timestamp()
        self.redis.set(f'{key}:{bucket_name}', json.dumps(bucket))
        self.redis.sadd(key, bucket_name)

    def delete_bucket(self, instance_id, bucket_name):
        for suffix in ['public', 'private']:
            key = f'{self.key_prefix}:instances:{instance_id}:buckets:{suffix}'
            self.redis.srem(key, bucket_name)
            self.redis.delete(f'{key}:{bucket_name}')

    def add_file(self, instance_id, bucket_name, filename, content_length):
        self.redis.set(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files:{filename}', f'{content_length}:{self.get_timestamp()}')
        self.redis.sadd(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files', filename)

    def delete_file(self, instance_id, bucket_name, filename):
        self.redis.srem(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files', filename)
        self.redis.delete(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files:{filename}')

    def get_bucket_names(self, instance_id, public, ttl_seconds=30):
        suffix = 'public' if public else 'private'
        key = f'{self.key_prefix}:instances:{instance_id}:buckets:{suffix}'
        res = []
        now = self.get_timestamp()
        for name in self.redis.smembers(key):
            name = name.decode('utf-8')
            if ttl_seconds:
                bucket = self.redis.get(f'{key}:{name}')
                if bucket:
                    bucket = json.loads(bucket)
                    if self.seconds_since(bucket.get('__ts'), now) > ttl_seconds:
                        res.append(name)
            else:
                res.append(name)
        return res

    def get_instance_ids(self, ttl_seconds=30):
        res = []
        for instance_id in self.redis.smembers(f'{self.key_prefix}:instances'):
            instance_id = instance_id.decode('utf-8')
            if ttl_seconds:
                data = self.redis.get(f'{self.key_prefix}:instances:{instance_id}')
                if data:
                    if self.seconds_since(data.decode('utf-8').split(':')[2]) > ttl_seconds:
                        res.append(instance_id)
            else:
                res.append(instance_id)
        return res

    def get_instance(self, instance_id):
        data = self.redis.get(f'{self.key_prefix}:instances:{instance_id}')
        if data:
            access_key, secret_key, _ = data.decode('utf-8').split(':')
            return access_key, secret_key
        else:
            return None

    def get_filenames(self, instance_id, bucket_name, ttl_seconds=30):
        key = f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files'
        res = []
        now = self.get_timestamp()
        for filename in self.redis.smembers(key):
            filename = filename.decode('utf-8')
            if ttl_seconds:
                data = self.redis.get(f'{key}:{filename}')
                if data:
                    if self.seconds_since(data.decode('utf-8').split(':')[1], now) > ttl_seconds:
                        res.append(filename)
            else:
                res.append(filename)
        return res

    def get_filename_seconds_since_created(self, instance_id, bucket_name, filename):
        key = f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files:{filename}'
        data = self.redis.get(key)
        if data:
            _, ts = data.decode('utf-8').split(':')
            return self.seconds_since(ts)
        else:
            return None
