from locust import events
from locust.runners import MasterRunner, LocalRunner


from cwm_minio_api.load_tests import config, shared_state

if config.CWM_GETGETTER_ENABLED:
    from cwm_minio_api.load_tests.users.getgetter import GetGetter

if config.CWM_UPDOWNDEL_ENABLED:
    from cwm_minio_api.load_tests.users.updowndel import UpDownDel

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    if isinstance(environment.runner, (MasterRunner,LocalRunner)):
        shared_state.clear()
