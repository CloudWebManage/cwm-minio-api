import hashlib
import uuid
import random

from locust import task
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials

from .base import BaseUser
from .. import config


class UploaderDownloader(BaseUser):

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
        res.raise_for_status()
        self.test_bucket_filenames.setdefault(bucket_name, {}).setdefault(content_length, []).append(filename)
        return filename

    def download_from_bucket_filename(self, bucket_name, filename, is_public=False):
        url = f'{self.minio_api_url}/{bucket_name}/{filename}'
        if is_public:
            res = self.client.get(url)
            res.raise_for_status()
        else:
            payload_hash = hashlib.sha256(b"").hexdigest()
            request = AWSRequest(method="GET", url=url, headers={"x-amz-content-sha256": payload_hash})
            SigV4Auth(Credentials(self.instance_access_key, self.instance_secret_key), "s3", "us-east-1").add_auth(request)
            res = self.client.get(url, headers=dict(request.headers))
            res.raise_for_status()

    def get_test_bucket_name(self):
        is_public = random.choices([True, False], weights=[config.CWM_UPLOADER_DOWNLOADER_PUBLIC_WEIGHT, config.CWM_UPLOADER_DOWNLOADER_PRIVATE_WEIGHT], k=1)[0]
        bucket_names = [bucket_name for bucket_name, bucket in self.instance_buckets.items() if bucket['public'] == is_public]
        if len(bucket_names) == 0:
            return self.create_bucket(is_public), is_public
        else:
            return random.choice(bucket_names), is_public

    def get_test_filenames(self, bucket_name):
        content_length = random.choices(config.CWM_UPLOADER_DOWNLOADER_CONTENT_LENGTH_VALUES, weights=config.CWM_UPLOADER_DOWNLOADER_CONTENT_LENGTH_WEIGHTS, k=1)[0]
        return [filename for filename in self.test_bucket_filenames.get(bucket_name, {}).get(content_length, [])], content_length

    @task(config.CWM_UPLOADER_DOWNLOADER_UPLOAD_WEIGHT)
    def upload(self):
        bucket_name, _ = self.get_test_bucket_name()
        filenames, content_length = self.get_test_filenames(bucket_name)
        if len(filenames) <= config.CWM_UPLOADER_DOWNLOADER_MAX_FILES_PER_BUCKET:
            self.upload_to_bucket(bucket_name, content_length)

    @task(config.CWM_UPLOADER_DOWNLOADER_DOWNLOAD_WEIGHT)
    def download(self):
        bucket_name, is_public = self.get_test_bucket_name()
        filenames, content_length = self.get_test_filenames(bucket_name)
        if filenames:
            filename = random.choice(filenames)
        else:
            filename = self.upload_to_bucket(bucket_name, content_length)
        self.download_from_bucket_filename(bucket_name, filename, is_public=is_public)
