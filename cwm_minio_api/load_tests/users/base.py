import uuid
import hashlib
import traceback
import json

import gevent
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
    max_retries = 0  # we don't rely on the built-in retries

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
        _, res_text = self.client_request_retry(
            'post',
            "/instances/create",
            json={"instance_id": self.instance_id},
            auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
            max_attempts=20,
        )
        instance = json.loads(res_text)
        self.instance_access_key = instance["access_key"]
        self.instance_secret_key = instance["secret_key"]
        self.shared_state.add_instance(self.instance_id, self.instance_access_key, self.instance_secret_key)

    def create_bucket(self, public=False):
        bucket_name = generate_bucket_name(public)
        print(f'Creating bucket: {bucket_name} (public={public},instance={self.instance_id})')
        self.client_request_retry(
            'post',
            "/buckets/create",
            json={"instance_id": self.instance_id, "bucket_name": bucket_name, "public": public},
            auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
            max_attempts=20,
        )
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
        errors = []
        if not config.CWM_KEEP_BUCKETS:
            print("Deleting buckets...")
            for public in [True, False]:
                for bucket_name in self.shared_state.get_bucket_names(self.instance_id, public=public, ttl_seconds=0):
                    print(f'Deleting bucket: {bucket_name} (instance={self.instance_id})')
                    try:
                        self.client_request_retry(
                            'delete',
                            "/buckets/delete",
                            params={"instance_id": self.instance_id, "bucket_name": bucket_name},
                            auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
                            should_retry=lambda res: (
                                res.status_code < 200 or res.status_code >= 300,
                                f'Failed to delete bucket {bucket_name} in instance {self.instance_id}: {res.status_code} {res.text}'
                            ),
                            max_attempts=20,
                            backoff=(1, 60, 2),
                        )
                        self.shared_state.delete_bucket(self.instance_id, bucket_name)
                    except Exception as e:
                        errors.append(f'Error deleting bucket {bucket_name} in instance {self.instance_id}: {str(e)}\n{traceback.format_exc()}')
        if not config.CWM_KEEP_INSTANCE and not config.CWM_KEEP_BUCKETS:
            print(f'Deleting instance: {self.instance_id}')
            try:
                self.client_request_retry(
                    'delete',
                    "/instances/delete",
                    params={"instance_id": self.instance_id},
                    auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
                    should_retry=lambda res: (
                        res.status_code < 200 or res.status_code >= 300,
                        f'Failed to delete instance {self.instance_id}: {res.status_code} {res.text}'
                    ),
                    max_attempts=20,
                    backoff=(1, 60, 2),
                )
                self.shared_state.delete_instance(self.instance_id)
            except Exception as e:
                errors.append(f'Error deleting instance {self.instance_id}: {str(e)}\n{traceback.format_exc()}')
        if errors:
            raise Exception("Errors during teardown:\n" + "\n".join(errors))
        print("Teardown complete.")

    def download_from_bucket_filename(self, bucket_name, filename, is_public=False):
        url = f'{self.minio_api_url}/{bucket_name}/{filename}'
        headers = {}
        if not is_public:
            payload_hash = hashlib.sha256(b"").hexdigest()
            request = AWSRequest(method="GET", url=url, headers={"x-amz-content-sha256": payload_hash})
            SigV4Auth(Credentials(self.instance_access_key, self.instance_secret_key), "s3", "us-east-1").add_auth(request)
            headers=dict(request.headers)

        def should_retry(res):
            if res.status_code == 404:
                if self.shared_state.is_filename_exists(self.instance_id, bucket_name, filename):
                    return True, f'file not found'
            return False, None

        self.client_request_retry(
            'get',
            url,
            headers=headers,
            should_retry=should_retry,
        )

    def client_request_retry(self, client_method, *args, max_attempts=10, backoff=(1, 20, 2), should_retry=None, **kwargs):
        last_error_msg = None
        for attempt in range(1, max_attempts + 1):
            with getattr(self.client, client_method)(*args, catch_response=True, **kwargs) as res:
                if should_retry:
                    retry, msg = should_retry(res)
                else:
                    retry = res.status_code in (404, 503)
                    msg = f'{res.status_code}'
                if retry:
                    if attempt < max_attempts:
                        res.failure(f'client_request_retry: {client_method}: {msg} ({attempt}/{max_attempts})')
                        sleep_time = min(backoff[1], backoff[0] * (backoff[2] ** (attempt - 1)))
                        gevent.sleep(sleep_time)
                        continue
                    else:
                        res.failure(f'client_request_retry: {client_method}: {msg} (exceeded max attempts {max_attempts})')
                        last_error_msg = f'{msg}\n{res.status_code} {res.text}'
                        break
                else:
                    return res.status_code, res.text
        raise Exception(f'client_request_retry: exceeded max attempts {max_attempts}: {last_error_msg}')
