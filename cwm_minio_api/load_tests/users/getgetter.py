import random

from locust import task

from .base import BaseUser
from .. import config


class GetGetter(BaseUser):
    if config.CWM_GETGETTER_FIXED_COUNT > 0:
        fixed_count = config.CWM_GETGETTER_FIXED_COUNT
    else:
        weight = config.CWM_GETGETTER_USER_WEIGHT

    def __init__(self, environment):
        super().__init__(environment)
        self.num_all_instances = 0

    @task
    def get(self):
        self.assign_instance()
        if self.instance_id:
            is_public = random.choices([True, False], weights=[config.CWM_UPDOWNDEL_PUBLIC_WEIGHT, config.CWM_UPDOWNDEL_PRIVATE_WEIGHT], k=1)[0]
            bucket_names = self.shared_state.get_bucket_names(self.instance_id, is_public)
            if len(bucket_names) > 0:
                bucket_name = random.choice(bucket_names)
                filenames = self.shared_state.get_filenames(self.instance_id, bucket_name)
                if len(filenames) > 0:
                    filename = random.choice(filenames)
                    self.download_from_bucket_filename(bucket_name, filename, is_public)

    def assign_instance(self):
        all_instance_ids = self.shared_state.get_instance_ids()
        if len(all_instance_ids) > 0:
            if self.num_all_instances != len(all_instance_ids):
                instance_id = random.choice(all_instance_ids)
                instance = self.shared_state.get_instance(instance_id)
                if instance:
                    self.instance_access_key, self.instance_secret_key = instance
                    self.instance_id = instance_id
                    print(f'GetGetter using instance: {self.instance_id} (total instances: {len(all_instance_ids)})')
                    self.num_all_instances = len(all_instance_ids)

    def on_start(self):
        self.update_tenant_info()
        self.assign_instance()

    def on_stop(self):
        pass
