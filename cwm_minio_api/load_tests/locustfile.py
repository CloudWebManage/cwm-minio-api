from gevent import monkey, sleep
monkey.patch_all()

from locust import events, LoadTestShape
from locust.runners import MasterRunner, LocalRunner

from cwm_minio_api.load_tests.users.getgetter import GetGetter
from cwm_minio_api.load_tests.users.updowndel import UpDownDel
from cwm_minio_api.load_tests import config, shared_state


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    environment.shared_state = shared_state.SharedState()
    if isinstance(environment.runner, (MasterRunner,LocalRunner)):
        environment.shared_state.clear()
    environment.num_updowndel_onstart_completed = 0


@events.test_stop.add_listener
def on_stop(environment, **kwargs):
    if isinstance(environment.runner, (MasterRunner, LocalRunner)):
        updowndel_started = environment.shared_state.counter_get('updowndel_started')
        while updowndel_started > environment.shared_state.counter_get('updowndel_stopped'):
            print('master waiting for all users to stop...')
            sleep(1)
        print('all users stopped')


class CwmLoadTestShape(LoadTestShape):
    use_common_options = True

    def __init__(self):
        super().__init__()
        self.updowndel_separate_initialized = False
        self.num_users, self.spawn_rate = None, None

    def tick(self):
        if self.num_users is None or self.spawn_rate is None:
            self.num_users = self.runner.environment.parsed_options.users
            self.spawn_rate = self.runner.environment.parsed_options.spawn_rate
        print(f'tick: num_users={self.num_users}, spawn_rate={self.spawn_rate}, runner={self.runner.__class__.__name__}')
        num_users, spawn_rate = self.num_users, self.spawn_rate
        user_classes = set()
        if config.CWM_GETGETTER_ENABLED:
            user_classes.add(GetGetter)
        if config.CWM_UPDOWNDEL_ENABLED:
            user_classes.add(UpDownDel)
        if config.CWM_UPDOWNDEL_SEPARATE_FROM_OTHER_USERS:
            if not self.updowndel_separate_initialized:
                assert UpDownDel in user_classes and len(user_classes) > 1 and UpDownDel.fixed_count > 0
                if hasattr(self.runner.environment, 'shared_state'):
                    self.updowndel_separate_initialized = self.runner.environment.shared_state.counter_get('updowndel_started') >= UpDownDel.fixed_count
            if self.updowndel_separate_initialized:
                user_classes.remove(UpDownDel)
            else:
                num_users = UpDownDel.fixed_count
                user_classes = {UpDownDel}
        print(f'tick res: num_users={num_users}, spawn_rate={spawn_rate}, user_classes={[cls.__name__ for cls in user_classes]}')
        return num_users, spawn_rate, list(user_classes)
