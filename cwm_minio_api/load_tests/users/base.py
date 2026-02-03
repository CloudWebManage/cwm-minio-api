import uuid
import time
import hashlib
import traceback
import random

from locust import FastHttpUser
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials

from .. import config
from ..shared_state import SharedState


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
        self.shared_state = SharedState()
        self.instance_id = None
        self.instance_access_key = None
        self.instance_secret_key = None
        self.tenant_info = {}

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
        if res.status_code < 200 or res.status_code >= 300:
            raise Exception(f'Failed to create instance {self.instance_id}: {res.status_code} {res.text}')
        instance = res.json()
        self.instance_access_key = instance["access_key"]
        self.instance_secret_key = instance["secret_key"]
        self.shared_state.add_instance(self.instance_id, self.instance_access_key, self.instance_secret_key)

    def create_bucket(self, public=False):
        bucket_name = generate_bucket_name(public)
        print(f'Creating bucket: {bucket_name} (public={public},instance={self.instance_id})')
        res = self.client.post(
            f"/buckets/create",
            json={"instance_id": self.instance_id, "bucket_name": bucket_name, "public": public},
            auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
        )
        if res.status_code < 200 or res.status_code >= 300:
            raise Exception(f'Failed to create bucket {bucket_name} in instance {self.instance_id}: {res.status_code} {res.text}')
        bucket = {
            "created": True,
            "public": public,
        }
        self.shared_state.upsert_bucket(self.instance_id, bucket_name, bucket)
        return bucket_name

    def update_tenant_info(self):
        self.tenant_info.update(**self.client.get(
            f'/tenant/info',
            auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
        ).json())

    def on_start(self):
        self.update_tenant_info()
        self.create_instance()

    def on_stop(self):
        try_num = 1
        max_tries = 20
        while True:
            try:
                if not config.CWM_KEEP_BUCKETS:
                    print("Deleting buckets...")
                    for public in [True, False]:
                        for bucket_name in self.shared_state.get_bucket_names(self.instance_id, public=public, ttl_seconds=0):
                            print(f'Deleting bucket: {bucket_name} (instance={self.instance_id})')
                            res = self.client.delete(
                                f"/buckets/delete",
                                params={"instance_id": self.instance_id, "bucket_name": bucket_name},
                                auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
                            )
                            if res.status_code < 200 or res.status_code >= 300:
                                raise Exception(f'Failed to delete bucket {bucket_name} in instance {self.instance_id}: {res.status_code} {res.text}')
                            self.shared_state.delete_bucket(self.instance_id, bucket_name)
                if not config.CWM_KEEP_INSTANCE and not config.CWM_KEEP_BUCKETS:
                    print(f'Deleting instance: {self.instance_id}')
                    res = self.client.delete(
                        f"/instances/delete",
                        params={"instance_id": self.instance_id},
                        auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
                    )
                    if res.status_code < 200 or res.status_code >= 300:
                        raise Exception(f'Failed to delete instance {self.instance_id}: {res.status_code} {res.text}')
                    self.shared_state.delete_instance(self.instance_id)
            except Exception:
                if try_num >= max_tries:
                    raise
                traceback.print_exc()
                print(f"Error during teardown, retrying... ({try_num}/{max_tries})")
                try_num += 1
                time.sleep(random.uniform(1, 3))
            else:
                break
        print("Teardown complete.")

    def client_get(self, url, errormsg, **kwargs):
        with self.client.get(url, catch_response=True, **kwargs) as res:
            if res.status_code < 200 or res.status_code >= 300:
                res.failure(f'{errormsg}: {res.status_code} {res.text}')
            else:
                res.success()

    def download_from_bucket_filename(self, bucket_name, filename, is_public=False):
        url = f'{self.minio_api_url}/{bucket_name}/{filename}'
        if is_public:
            self.client_get(url, f'Failed to download file {filename} from public bucket {bucket_name}')
        else:
            payload_hash = hashlib.sha256(b"").hexdigest()
            request = AWSRequest(method="GET", url=url, headers={"x-amz-content-sha256": payload_hash})
            SigV4Auth(Credentials(self.instance_access_key, self.instance_secret_key), "s3", "us-east-1").add_auth(request)
            self.client_get(url, f'Failed to download file {filename} from private bucket {bucket_name}', headers=dict(request.headers))
