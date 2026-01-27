import uuid
from locust import task, FastHttpUser

from .. import config


def generate_instance_id():
    return f"cmaloadtest-{uuid.uuid4()}"


class BaseUser(FastHttpUser):
    host = "http://127.0.0.1"  # we set our own hosts in requests

    def __init__(self, environment):
        super().__init__(environment)
        assert config.CWM_MINIO_API_URL and config.CWM_MINIO_API_USERNAME and config.CWM_MINIO_API_PASSWORD
        self.instance_id = None
        self.instance_access_key = None
        self.instance_secret_key = None
        if config.CWM_INSTANCE_ID:
            self.instance_id = config.CWM_INSTANCE_ID
            assert config.CWM_INSTANCE_ACCESS_KEY and config.CWM_INSTANCE_SECRET_KEY
            self.instance_access_key = config.CWM_INSTANCE_ACCESS_KEY
            self.instance_secret_key = config.CWM_INSTANCE_SECRET_KEY

    def create_instance(self):
        self.instance_id = generate_instance_id()
        res = self.client.post(
            f"{config.CWM_MINIO_API_URL}/instances/create",
            json={
                "instance_id": self.instance_id
            },
            auth=(config.CWM_MINIO_API_USERNAME, config.CWM_MINIO_API_PASSWORD),
        )
        res.raise_for_status()
        instance = res.json()
        self.instance_access_key = instance["access_key"]
        self.instance_secret_key = instance["secret_key"]

    def on_start(self):
        if not self.instance_id:
            self.create_instance()
