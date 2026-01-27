import uuid
from locust import task, FastHttpUser

from .. import config


def generate_instance_id():
    return f"cmalti-{uuid.uuid4()}"


def generate_bucket_name(public):
    suffix = "pub" if public else "priv"
    return f"cmaltb{suffix}-{uuid.uuid4()}"


class BaseUser(FastHttpUser):
    host = f'https://{config.CWM_MINIO_API_HOST}'
    insecure = False

    def __init__(self, environment):
        super().__init__(environment)
        assert config.CWM_MINIO_API_USERNAME and config.CWM_MINIO_API_PASSWORD
        self.instance_id = None
        self.instance_access_key = None
        self.instance_secret_key = None
        self.existing_instance = False
        self.instance_buckets = {}
        self.tenant_info = {}
        if config.CWM_INSTANCE_ID:
            self.existing_instance = True
            self.instance_id = config.CWM_INSTANCE_ID
            assert config.CWM_INSTANCE_ACCESS_KEY and config.CWM_INSTANCE_SECRET_KEY
            self.instance_access_key = config.CWM_INSTANCE_ACCESS_KEY
            self.instance_secret_key = config.CWM_INSTANCE_SECRET_KEY

    @property
    def minio_api_url(self):
        return self.tenant_info["api_url"].rstrip('/')

    def create_instance(self):
        self.instance_id = generate_instance_id()
        print(f'Creating instance: {self.instance_id}')
        res = self.client.post(
            f"/instances/create",
            json={
                "instance_id": self.instance_id
            },
            auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
        )
        res.raise_for_status()
        instance = res.json()
        self.instance_access_key = instance["access_key"]
        self.instance_secret_key = instance["secret_key"]

    def update_instance_buckets(self):
        print(f'Updating instance buckets: {self.instance_id}')
        res = self.client.get(
            f"/buckets/list",
            params={"instance_id": self.instance_id},
            auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
        )
        res.raise_for_status()
        for name in res.json():
            res = self.client.get(
                f"/buckets/get",
                params={"instance_id": self.instance_id, "bucket_name": name,},
                auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
            )
            bucket = res.json()
            self.instance_buckets[name] = {
                "created": False,
                "public": bucket["public"]
            }

    def create_bucket(self, public=False):
        bucket_name = generate_bucket_name(public)
        print(f'Creating bucket: {bucket_name} (public={public},instance={self.instance_id})')
        res = self.client.post(
            f"/buckets/create",
            json={"instance_id": self.instance_id, "bucket_name": bucket_name, "public": public},
            auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
        )
        res.raise_for_status()
        self.instance_buckets[bucket_name] = {
            "created": True,
            "public": public,
        }
        return bucket_name

    def update_tenant_info(self):
        self.tenant_info.update(**self.client.get(
            f'/tenant/info',
            auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
        ).json())

    def on_start(self):
        self.update_tenant_info()
        if self.existing_instance:
            self.update_instance_buckets()
        else:
            self.create_instance()
        if len(self.instance_buckets) == 0:
            self.create_bucket()

    def on_stop(self):
        for bucket_name, bucket in self.instance_buckets.items():
            if bucket["created"] and not config.CWM_KEEP_BUCKETS:
                print(f'Deleting bucket: {bucket_name} (instance={self.instance_id})')
                res = self.client.delete(
                    f"/buckets/delete",
                    params={"instance_id": self.instance_id, "bucket_name": bucket_name},
                    auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
                )
                res.raise_for_status()
        if not self.existing_instance and not config.CWM_KEEP_INSTANCE and not config.CWM_KEEP_BUCKETS:
            print(f'Deleting instance: {self.instance_id}')
            res = self.client.delete(
                f"/instances/delete",
                params={"instance_id": self.instance_id},
                auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
            )
            res.raise_for_status()
