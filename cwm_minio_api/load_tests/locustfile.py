import logging

from gevent import monkey, sleep
monkey.patch_all()

import requests
from locust import events, LoadTestShape
from locust.runners import MasterRunner, LocalRunner

from cwm_minio_api.load_tests.users.getgetter import GetGetter
from cwm_minio_api.load_tests.users.updowndel import UpDownDel
from cwm_minio_api.load_tests.users.base import teardown_instance
from cwm_minio_api.load_tests import config, shared_state


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    environment.shared_state = shared_state.SharedState()
    if isinstance(environment.runner, (MasterRunner,LocalRunner)):
        environment.shared_state.clear()
    environment.num_updowndel_onstart_completed = 0


def independent_client_request_retry(method, path, params, auth, **kwargs):
    url = f'https://{config.CWM_MINIO_API_HOST}/{path}'
    res = requests.request(method, url, params=params, auth=auth)
    if res.status_code < 200 or res.status_code >= 300:
        logging.error(f'Error during client request {method} {path} {params}: {res.status_code} {res.text}')


@events.test_stop.add_listener
def on_stop(environment, **kwargs):
    if isinstance(environment.runner, (MasterRunner, LocalRunner)):
        updowndel_started = environment.shared_state.counter_get('updowndel_started')
        logging.info(f'master waiting for {updowndel_started} users to stop...')
        while updowndel_started > environment.shared_state.counter_get('updowndel_stopped'):
            sleep(1)
        logging.info('all users stopped, starting teardown...')
        instance_ids = environment.shared_state.get_instance_ids(ttl_seconds=0)
        for instance_id in instance_ids:
            teardown_instance(environment.shared_state, instance_id, independent_client_request_retry)
        logging.info(f'test teardown complete, {len(instance_ids)} instances deleted.')


class CwmLoadTestShape(LoadTestShape):
    use_common_options = True

    def __init__(self):
        super().__init__()
        self.updowndel_separate_initialized = False
        self.updowndel_separate_scaled_down = False
        self.num_users, self.spawn_rate = None, None

    def tick(self):
        if self.num_users is None or self.spawn_rate is None:
            self.num_users = self.runner.environment.parsed_options.users
            self.spawn_rate = self.runner.environment.parsed_options.spawn_rate
        num_users, spawn_rate = self.num_users, self.spawn_rate
        user_classes = set()
        if config.CWM_GETGETTER_ENABLED:
            user_classes.add(GetGetter)
        if config.CWM_UPDOWNDEL_ENABLED:
            user_classes.add(UpDownDel)
        if config.CWM_UPDOWNDEL_SEPARATE_FROM_OTHER_USERS:
            if not self.updowndel_separate_initialized:
                assert UpDownDel in user_classes and len(user_classes) > 1 and UpDownDel.fixed_count > 0
                if not hasattr(self.runner.environment, 'shared_state') or self.runner.environment.shared_state.counter_get('updowndel_started') < UpDownDel.fixed_count:
                    # Initial spawn of fixed UpDownDel users
                    return UpDownDel.fixed_count, spawn_rate, user_classes
                else:
                    # Mark that initial UpDownDel users have been spawned
                    # Start scale down to 0 before starting other users
                    self.updowndel_separate_initialized = True
                    return 0, spawn_rate, [UpDownDel]
            elif not self.updowndel_separate_scaled_down:
                if self.runner.user_count > 0:
                    # Scale down UpDownDel users to 0 first
                    return 0, spawn_rate, [UpDownDel]
                else:
                    # all UpDownDel users scaled down to 0, now start other users
                    self.updowndel_separate_scaled_down = True
                    return num_users, spawn_rate, list(user_classes - {UpDownDel})
            else:
                # All UpDownDel users scaled down to 0, run only other users
                return num_users, spawn_rate, list(user_classes - {UpDownDel})
        else:
            return num_users, spawn_rate, list(user_classes)
