import hashlib
import uuid
import random
from textwrap import dedent
import base64

from locust import task
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials

from .base import BaseUser
from .. import config


class UpDownDel(BaseUser):
    if config.CWM_UPDOWNDEL_FIXED_COUNT > 0:
        fixed_count = config.CWM_UPDOWNDEL_FIXED_COUNT
    else:
        weight = config.CWM_UPDOWNDEL_USER_WEIGHT

    concurrency = config.CWM_UPDOWNDEL_CONCURRENCY

    def __init__(self, environment):
        super().__init__(environment)

    def on_start(self):
        super().on_start()
        self.debug(f'UpDownDel on_start (instance_id: {self.instance_id})')
        bucket_names = set()
        for i in range(config.CWM_UPDOWNDEL_ON_START_NUM_BUCKETS):
            is_public = random.choices([True, False], weights=[config.CWM_UPDOWNDEL_PUBLIC_WEIGHT, config.CWM_UPDOWNDEL_PRIVATE_WEIGHT], k=1)[0]
            bucket_names.add(self.create_bucket(public=is_public))
        self.debug(f'UpDownDel on_start (instance_id: {self.instance_id}): created {len(bucket_names)} buckets')
        num_files = 0
        for i in range(config.CWM_UPDOWNDEL_ON_START_NUM_FILES_PER_BUCKET):
            for bucket_name in bucket_names:
                content_length = random.choices(config.CWM_UPDOWNDEL_CONTENT_LENGTH_VALUES, weights=config.CWM_UPDOWNDEL_CONTENT_LENGTH_WEIGHTS, k=1)[0]
                self.upload_to_bucket(bucket_name, content_length)
                num_files += 1
        self.debug(f'UpDownDel on_start (instance_id: {self.instance_id}): uploaded {num_files} files')
        self.shared_state.counter_incr('updowndel_started')

    def on_stop(self):
        super().on_stop()
        self.shared_state.counter_incr('updowndel_stopped')

    def upload_to_bucket(self, bucket_name, content_length):
        body = b"a" * content_length
        filename = f'{uuid.uuid4().hex}-{content_length}.md'
        url = f'{self.minio_api_url}/{bucket_name}/{filename}'
        payload_hash = hashlib.sha256(body).hexdigest()
        request = AWSRequest(method="PUT", url=url, data=body, headers={"x-amz-content-sha256": payload_hash})
        SigV4Auth(Credentials(self.instance_access_key, self.instance_secret_key), "s3", "us-east-1").add_auth(request)
        self.client_request_retry(
            'put',
            url,
            headers=dict(request.headers),
            data=body,
            name=f'upload_to_bucket({content_length})',
        )
        self.shared_state.add_file(self.instance_id, bucket_name, filename, content_length)
        return filename

    def delete_from_bucket(self, bucket_name, filename):
        self.shared_state.delete_file(self.instance_id, bucket_name, filename)
        url = f'{self.minio_api_url}/{bucket_name}/{filename}'
        payload_hash = hashlib.sha256(b"").hexdigest()
        request = AWSRequest(method="DELETE", url=url, headers={"x-amz-content-sha256": payload_hash})
        SigV4Auth(Credentials(self.instance_access_key, self.instance_secret_key), "s3", "us-east-1").add_auth(request)
        self.client_request_retry(
            'delete',
            url,
            headers=dict(request.headers),
            name='delete_from_bucket',
        )

    def delete_from_bucket_multi(self, bucket_name, filenames):
        for filename in filenames:
            self.shared_state.delete_file(self.instance_id, bucket_name, filename)
        body = dedent(f'''
        <Delete>
            {''.join([f'<Object><Key>{filename}</Key></Object>' for filename in filenames])}
            <Quiet>true</Quiet>
        </Delete>
        ''').encode('utf-8')
        url = f"{self.minio_api_url}/{bucket_name}/?delete="
        body_md5 = base64.b64encode(hashlib.md5(body).digest()).decode("ascii")
        payload_hash = hashlib.sha256(body).hexdigest()
        headers = {
            "Content-MD5": body_md5,
            "Content-Type": "application/xml",
            "x-amz-content-sha256": payload_hash,
        }
        request = AWSRequest(method="POST", url=url, headers=headers, data=body)
        SigV4Auth(Credentials(self.instance_access_key, self.instance_secret_key), "s3", "us-east-1").add_auth(request)
        self.client_request_retry(
            'post',
            url,
            data=body.decode(),
            headers=dict(request.headers),
            name='delete_from_bucket_multi',
        )

    def get_test_bucket_name(self):
        is_public = random.choices([True, False], weights=[config.CWM_UPDOWNDEL_PUBLIC_WEIGHT, config.CWM_UPDOWNDEL_PRIVATE_WEIGHT], k=1)[0]
        bucket_names = self.shared_state.get_bucket_names(self.instance_id, is_public)
        if len(bucket_names) > 0:
            return random.choice(bucket_names), is_public
        else:
            return None, None

    def get_test_filenames(self, bucket_name):
        content_length = random.choices(config.CWM_UPDOWNDEL_CONTENT_LENGTH_VALUES, weights=config.CWM_UPDOWNDEL_CONTENT_LENGTH_WEIGHTS, k=1)[0]
        return self.shared_state.get_filenames(self.instance_id, bucket_name), content_length

    @task(config.CWM_UPDOWNDEL_UPLOAD_WEIGHT)
    def upload(self):
        bucket_name, _ = self.get_test_bucket_name()
        if bucket_name:
            filenames, content_length = self.get_test_filenames(bucket_name)
            if len(filenames) <= config.CWM_UPDOWNDEL_MAX_FILES_PER_BUCKET:
                self.upload_to_bucket(bucket_name, content_length)

    @task(config.CWM_UPDOWNDEL_DOWNLOAD_WEIGHT)
    def download(self):
        bucket_name, is_public = self.get_test_bucket_name()
        if bucket_name:
            filenames, content_length = self.get_test_filenames(bucket_name)
            if filenames:
                filename = random.choice(filenames)
                self.download_from_bucket_filename(bucket_name, filename, is_public=is_public)

    @task(config.CWM_UPDOWNDEL_DELETE_WEIGHT)
    def delete(self):
        bucket_name, is_public = self.get_test_bucket_name()
        if bucket_name:
            filenames, _ = self.get_test_filenames(bucket_name)
            if filenames and len(filenames) > config.CWM_UPDOWNDEL_MIN_FILES_PER_BUCKET:
                num_to_delete = random.randint(1, 3)
                if num_to_delete == 1:
                    filename = random.choice(filenames)
                    self.delete_from_bucket(bucket_name, filename)
                else:
                    filenames = random.sample(filenames, min(num_to_delete, config.CWM_UPDOWNDEL_MIN_FILES_PER_BUCKET))
                    self.delete_from_bucket_multi(bucket_name, filenames)
