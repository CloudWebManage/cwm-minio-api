import json

from redis import Redis

from . import config


class SharedState:
    def __init__(self):
        self.redis = Redis(config.SHARED_STATE_REDIS_HOST, config.SHARED_STATE_REDIS_PORT)
        self.key_prefix = 'cwm-minio-api:load-tests'

    def add_instance(self, instance_id, instance_access_key, instance_secret_key):
        self.redis.set(f'{self.key_prefix}:instances:{instance_id}', f'{instance_access_key}:{instance_secret_key}')
        self.redis.sadd(f'{self.key_prefix}:instances', instance_id)

    def delete_instance(self, instance_id):
        self.redis.srem(f'{self.key_prefix}:instances', instance_id)
        self.redis.delete(f'{self.key_prefix}:instances:{instance_id}')

    def upsert_bucket(self, instance_id, bucket_name, bucket):
        key = f'{self.key_prefix}:instances:{instance_id}:buckets:'
        key += 'public' if bucket["public"] else 'private'
        self.redis.set(f'{key}:{bucket_name}', json.dumps(bucket))
        self.redis.sadd(key, bucket_name)

    def delete_bucket(self, instance_id, bucket_name):
        for suffix in ['public', 'private']:
            key = f'{self.key_prefix}:instances:{instance_id}:buckets:{suffix}'
            self.redis.srem(key, bucket_name)
            self.redis.delete(f'{key}:{bucket_name}')

    def add_file(self, instance_id, bucket_name, filename, content_length):
        self.redis.set(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files:{filename}', content_length)
        self.redis.sadd(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files', filename)

    def delete_file(self, instance_id, bucket_name, filename):
        self.redis.srem(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files', filename)
        self.redis.delete(f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files:{filename}')

    def get_bucket_names(self, instance_id, public):
        suffix = 'public' if public else 'private'
        key = f'{self.key_prefix}:instances:{instance_id}:buckets:{suffix}'
        return [name.decode('utf-8') for name in self.redis.smembers(key)]

    def get_instance_ids(self):
        return [instance_id.decode('utf-8') for instance_id in self.redis.smembers(f'{self.key_prefix}:instances')]

    def get_instance(self, instance_id):
        data = self.redis.get(f'{self.key_prefix}:instances:{instance_id}')
        if data:
            access_key, secret_key = data.decode('utf-8').split(':', 1)
            return access_key, secret_key
        else:
            return None

    def get_filenames(self, instance_id, bucket_name):
        key = f'{self.key_prefix}:instances:{instance_id}:buckets:{bucket_name}:files'
        return [filename.decode('utf-8') for filename in self.redis.smembers(key)]
