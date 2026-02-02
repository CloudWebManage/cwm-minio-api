import hashlib
import uuid
import random

from locust import task
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials

from .base import BaseUser
from .. import config


class UpDownDel(BaseUser):
    weight = config.CWM_UPDOWNDEL_USER_WEIGHT

    def __init__(self, environment):
        super().__init__(environment)
        self.test_bucket_filenames = {}

    def upload_to_bucket(self, bucket_name, content_length):
        body = b"a" * content_length
        filename = f'{uuid.uuid4().hex}-{content_length}.md'
        url = f'{self.minio_api_url}/{bucket_name}/{filename}'
        payload_hash = hashlib.sha256(body).hexdigest()
        request = AWSRequest(method="PUT", url=url, data=body, headers={"x-amz-content-sha256": payload_hash})
        SigV4Auth(Credentials(self.instance_access_key, self.instance_secret_key), "s3", "us-east-1").add_auth(request)
        res = self.client.put(url, headers=dict(request.headers), data=body)
        if res.status_code < 200 or res.status_code >= 300:
            raise Exception(f'Failed to upload file {filename} to bucket {bucket_name}: {res.status_code} {res.text}')
        self.test_bucket_filenames.setdefault(bucket_name, {}).setdefault(content_length, []).append(filename)
        self.shared_state.add_file(self.instance_id, bucket_name, filename, content_length)
        return filename

    def delete_from_bucket(self, bucket_name, filename):
        self.shared_state.delete_file(self.instance_id, bucket_name, filename)
        url = f'{self.minio_api_url}/{bucket_name}/{filename}'
        payload_hash = hashlib.sha256(b"").hexdigest()
        request = AWSRequest(method="DELETE", url=url, headers={"x-amz-content-sha256": payload_hash})
        SigV4Auth(Credentials(self.instance_access_key, self.instance_secret_key), "s3", "us-east-1").add_auth(request)
        res = self.client.delete(url, headers=dict(request.headers))
        if res.status_code < 200 or res.status_code >= 300:
            raise Exception(f'Failed to delete file {filename} from bucket {bucket_name}: {res.status_code} {res.text}')
        for content_length, filenames in self.test_bucket_filenames.get(bucket_name, {}).items():
            if filename in filenames:
                filenames.remove(filename)
                break

    def get_test_bucket_name(self):
        is_public = random.choices([True, False], weights=[config.CWM_UPDOWNDEL_PUBLIC_WEIGHT, config.CWM_UPDOWNDEL_PRIVATE_WEIGHT], k=1)[0]
        bucket_names = [bucket_name for bucket_name, bucket in self.instance_buckets.items() if bucket['public'] == is_public]
        if len(bucket_names) == 0:
            return self.create_bucket(is_public), is_public
        else:
            return random.choice(bucket_names), is_public

    def get_test_filenames(self, bucket_name):
        content_length = random.choices(config.CWM_UPDOWNDEL_CONTENT_LENGTH_VALUES, weights=config.CWM_UPDOWNDEL_CONTENT_LENGTH_WEIGHTS, k=1)[0]
        return [filename for filename in self.test_bucket_filenames.get(bucket_name, {}).get(content_length, [])], content_length

    @task(config.CWM_UPDOWNDEL_UPLOAD_WEIGHT)
    def upload(self):
        bucket_name, _ = self.get_test_bucket_name()
        filenames, content_length = self.get_test_filenames(bucket_name)
        if len(filenames) <= config.CWM_UPDOWNDEL_MAX_FILES_PER_BUCKET:
            self.upload_to_bucket(bucket_name, content_length)

    @task(config.CWM_UPDOWNDEL_DOWNLOAD_WEIGHT)
    def download(self):
        bucket_name, is_public = self.get_test_bucket_name()
        filenames, content_length = self.get_test_filenames(bucket_name)
        if filenames:
            filename = random.choice(filenames)
        else:
            filename = self.upload_to_bucket(bucket_name, content_length)
        self.download_from_bucket_filename(bucket_name, filename, is_public=is_public)

    @task(config.CWM_UPDOWNDEL_DELETE_WEIGHT)
    def delete(self):
        bucket_name, is_public = self.get_test_bucket_name()
        filenames, content_length = self.get_test_filenames(bucket_name)
        if filenames and len(filenames) > config.CWM_UPDOWNDEL_MIN_FILES_PER_BUCKET:
            filename = random.choice(filenames)
            self.delete_from_bucket(bucket_name, filename)
