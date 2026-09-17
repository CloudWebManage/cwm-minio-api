import fnmatch


def test_legacy_reset_preserves_other_redis_namespaces(monkeypatch):
    from cwm_minio_api.load_tests.shared_state import SharedState
    from cwm_minio_api.load_tests import config
    class RedisMemory:
        def __init__(self):
            self.keys = {"production:key", "cwm_objstore_loadtest:run:other", "cwm-minio-api:load-tests:instances:a"}
        def scan_iter(self, match, count):
            yield from [k for k in self.keys if fnmatch.fnmatch(k, match)]
        def unlink(self, *keys):
            self.keys.difference_update(keys)
    state = SharedState()
    state._redis = RedisMemory()
    monkeypatch.setattr(config, "CWM_KEEP_REDIS_DATA", False)
    state.clear()
    assert state.redis.keys == {"production:key", "cwm_objstore_loadtest:run:other"}
