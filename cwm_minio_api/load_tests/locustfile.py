import logging

from gevent import monkey, sleep
monkey.patch_all()

import requests
from locust import events, LoadTestShape
from locust.runners import MasterRunner, LocalRunner

from cwm_minio_api.load_tests.users.getgetter import GetGetter
from cwm_minio_api.load_tests.users.updowndel import UpDownDel
from cwm_minio_api.load_tests.users.base import teardown_instance
from cwm_minio_api.load_tests import config
from cwm_minio_api.load_tests.shared_state import SharedState


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    #logging.info("on_test_start")
    shared_state = SharedState.get_singleton()
    if isinstance(environment.runner, (MasterRunner,LocalRunner)) and not (config.CWM_INIT_FROM_JSON_FILE or config.CWM_INIT_FROM_REDIS):
        shared_state.clear()
    environment.num_updowndel_onstart_completed = 0
    #logging.info("on_test_start complete")


def independent_client_request_retry(method, path, params, auth, **kwargs):
    url = f'https://{config.CWM_MINIO_API_HOST}/{path}'
    res = requests.request(method, url, params=params, auth=auth)
    if res.status_code < 200 or res.status_code >= 300:
        logging.error(f'Error during client request {method} {path} {params}: {res.status_code} {res.text}')


@events.test_stop.add_listener
def on_stop(environment, **kwargs):
    logging.info("on_stop")
    if isinstance(environment.runner, (MasterRunner, LocalRunner)):
        shared_state = SharedState.get_singleton()
        updowndel_started = shared_state.counter_get('updowndel_started')
        logging.info(f'master waiting for {updowndel_started} users to stop...')
        while updowndel_started > shared_state.counter_get('updowndel_stopped'):
            sleep(1)
        logging.info('all users stopped, starting teardown...')
        instance_ids = shared_state.get_instance_ids(ttl_seconds=0)
        for instance_id in instance_ids:
            teardown_instance(shared_state, instance_id, independent_client_request_retry)
        logging.info(f'test teardown complete, {len(instance_ids)} instances deleted.')
        if not config.CWM_KEEP_REDIS_DATA:
            shared_state.clear()
            logging.info('shared state cleared.')
        if hasattr(environment, 'cwm_load_test_shape_state'):
            delattr(environment, 'cwm_load_test_shape_state')
    logging.info("on_stop complete")


class CwmLoadTestShape(LoadTestShape):
    use_common_options = True

    def tick(self):
        if not hasattr(self.runner.environment, 'cwm_load_test_shape_state'):
            self.runner.environment.cwm_load_test_shape_state = {
                'updowndel_separate_initialized': False,
                'updowndel_separate_scaled_down': False,
            }
        state = self.runner.environment.cwm_load_test_shape_state
        shared_state = SharedState.get_singleton()
        num_users, spawn_rate = self.runner.environment.parsed_options.users, self.runner.environment.parsed_options.spawn_rate
        user_classes = set()
        if config.CWM_GETGETTER_ENABLED:
            user_classes.add(GetGetter)
        if config.CWM_UPDOWNDEL_ENABLED:
            user_classes.add(UpDownDel)
        if config.CWM_UPDOWNDEL_SEPARATE_FROM_OTHER_USERS:
            if not state['updowndel_separate_initialized']:
                assert UpDownDel in user_classes and len(user_classes) > 1 and UpDownDel.fixed_count > 0
                if shared_state.counter_get('updowndel_started') < UpDownDel.fixed_count:
                    # Initial spawn of fixed UpDownDel users
                    return UpDownDel.fixed_count, spawn_rate, user_classes
                else:
                    # Mark that initial UpDownDel users have been spawned
                    # Start scale down to 0 before starting other users
                    state['updowndel_separate_initialized'] = True
                    return 0, spawn_rate, [UpDownDel]
            elif not state['updowndel_separate_scaled_down']:
                if self.runner.user_count > 0:
                    # Scale down UpDownDel users to 0 first
                    return 0, spawn_rate, [UpDownDel]
                else:
                    # all UpDownDel users scaled down to 0, now start other users
                    state['updowndel_separate_scaled_down'] = True
                    return num_users, spawn_rate, list(user_classes - {UpDownDel})
            else:
                # All UpDownDel users scaled down to 0, run only other users
                return num_users, spawn_rate, list(user_classes - {UpDownDel})
        else:
            return num_users, spawn_rate, list(user_classes)
