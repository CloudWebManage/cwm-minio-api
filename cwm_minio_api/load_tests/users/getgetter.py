import random

from locust import task

from .base import BaseUser
from .. import config


class GetGetter(BaseUser):
    if config.CWM_GETGETTER_FIXED_COUNT > 0:
        fixed_count = config.CWM_GETGETTER_FIXED_COUNT
    else:
        weight = config.CWM_GETGETTER_USER_WEIGHT

    concurrency = config.CWM_GETGETTER_CONCURRENCY

    def __init__(self, environment):
        super().__init__(environment)
        self.num_all_instances = 0

    @task
    def get(self):
        instance_id, access, secret = self.get_instance()
        if instance_id:
            is_public = random.choices([True, False], weights=[config.CWM_UPDOWNDEL_PUBLIC_WEIGHT, config.CWM_UPDOWNDEL_PRIVATE_WEIGHT], k=1)[0]
            bucket_names = self.shared_state.get_bucket_names(instance_id, is_public)
            if len(bucket_names) > 0:
                bucket_name = random.choice(bucket_names)
                filenames = self.shared_state.get_filenames(instance_id, bucket_name)
                if len(filenames) > 0:
                    filename = random.choice(filenames)
                    use_bucket_url = random.choices([True, False], weights=[100, 1], k=1)[0]
                    self.download_from_bucket_filename(
                        bucket_name, filename, is_public, use_bucket_url, instance=(instance_id, access, secret),
                        stream=True
                    )

    def get_instance(self):
        all_instance_ids = self.shared_state.get_instance_ids()
        if len(all_instance_ids) > 0:
            instance_id = random.choice(all_instance_ids)
            instance = self.shared_state.get_instance(instance_id)
            if instance:
                access, secret = instance
                return instance_id, access, secret
        return None, None, None

    def on_start(self):
        self.update_tenant_info()
