from types import SimpleNamespace
import pytest
from redis.exceptions import AuthenticationError,ConnectionError,ResponseError
from cwm_minio_api.load_tests.campaign.config import TransientFailure


def test_bounded_retry_recovers_transport_only():
    from cwm_minio_api.load_tests.campaign.distributed import redis_retry
    clock=[0.0];calls=[]
    def operation():
        calls.append(1)
        if len(calls)<3:raise ConnectionError('must not be published')
        return 'same logical operation'
    result=redis_retry(operation,3,clock=lambda:clock[0],sleep=lambda seconds:clock.__setitem__(0,clock[0]+seconds))
    assert result=='same logical operation' and len(calls)==3
    for error in [AuthenticationError('secret'),ResponseError('secret')]:
        calls.clear()
        def permanent():calls.append(1);raise error
        with pytest.raises(type(error)):redis_retry(permanent,3)
        assert len(calls)==1


def test_retry_exhaustion_is_bounded_and_sanitized():
    from cwm_minio_api.load_tests.campaign.distributed import redis_retry
    clock=[0.0]
    def unavailable():raise ConnectionError('SECRET')
    with pytest.raises(TransientFailure,match='coordination transport') as error:
        redis_retry(unavailable,2,clock=lambda:clock[0],sleep=lambda seconds:clock.__setitem__(0,clock[0]+seconds))
    assert clock[0]==2 and 'SECRET' not in str(error.value)
