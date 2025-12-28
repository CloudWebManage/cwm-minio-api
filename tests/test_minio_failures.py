import pytest

from cwm_minio_api.instances import api as instances_api
from cwm_minio_api.buckets import api as buckets_api


class MinioFailureException(Exception):
    pass


@pytest.mark.parametrize('rollback_failure', (
    (False,),
    (True,),
))
async def test_instance_create_failure(cwm_test_db, rollback_failure):
    instance_id = 'test_instance'

    async def intercept(f, n, *args):
        if n == "mc_check_call":
            if args[0:3] == ('admin', 'user', 'add'):
                raise MinioFailureException()
            if rollback_failure and args[0:3] == ('admin', 'user', 'rm'):
                raise MinioFailureException()
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    with pytest.raises(MinioFailureException):
        await instances_api.create(instance_id)
    cwm_test_db['tracker_assert_calls']([
        ('mc_check_call', 'admin', 'user', 'add'),
        ('mc_check_call', 'admin', 'user', 'rm'),
    ])
    assert [i async for i in instances_api.list_iterator()] == []


@pytest.mark.parametrize(
    'mb_failure,rb_failure,policy_create_failure,policy_delete_failure,policy_attach_failure,policy_detach_failure,anon_set_download_failure,anon_set_none_failure,public', (
    (True,      False,     False,                False,                False,                False,                False,                False,                False,),
    (True,      True,      False,                False,                False,                False,                False,                False,                False,),
    (False,     False,     True ,                False,                False,                False,                False,                False,                False,),
    (False,     False,     True ,                True ,                False,                False,                False,                False,                False,),
    (False,     False,     False,                False,                True ,                False,                False,                False,                False,),
    (False,     False,     False,                False,                True ,                True ,                False,                False,                False,),
    (True,      False,     False,                False,                False,                False,                False,                False,                True ,),
    (True,      True,      False,                False,                False,                False,                False,                False,                True ,),
    (False,     False,     True ,                False,                False,                False,                False,                False,                True ,),
    (False,     False,     True ,                True ,                False,                False,                False,                False,                True ,),
    (False,     False,     False,                False,                True ,                False,                False,                False,                True ,),
    (False,     False,     False,                False,                True ,                True ,                False,                False,                True ,),
    (True,      False,     False,                False,                False,                False,                True ,                False,                True ,),
    (True,      True,      False,                False,                False,                False,                True ,                False,                True ,),
    (False,     False,     True ,                False,                False,                False,                True ,                False,                True ,),
    (False,     False,     True ,                True ,                False,                False,                True ,                False,                True ,),
    (False,     False,     False,                False,                True ,                False,                True ,                False,                True ,),
    (False,     False,     False,                False,                True ,                True ,                True ,                False,                True ,),
    (True,      False,     False,                False,                False,                False,                True ,                True ,                True ,),
    (True,      True,      False,                False,                False,                False,                True ,                True ,                True ,),
    (False,     False,     True ,                False,                False,                False,                True ,                True ,                True ,),
    (False,     False,     True ,                True ,                False,                False,                True ,                True ,                True ,),
    (False,     False,     False,                False,                True ,                False,                True ,                True ,                True ,),
    (False,     False,     False,                False,                True ,                True ,                True ,                True ,                True ,),
))
async def test_bucket_create_failures(cwm_test_db,mb_failure,rb_failure,policy_create_failure,policy_delete_failure,policy_attach_failure,policy_detach_failure,anon_set_download_failure,anon_set_none_failure,public):
    instance_id = 'test_instance'
    bucket_name = 'bucket'
    await instances_api.create(instance_id)
    cwm_test_db['tracker_get_calls']()  # Clear calls

    async def intercept(f, n, *args):
        if n == "mc_check_call":
            if mb_failure and args[0] == 'mb':
                raise MinioFailureException()
            if rb_failure and args[0] == 'rb':
                raise MinioFailureException()
            if policy_create_failure and args[0:4] == ('admin', 'policy', 'create', 'cwm'):
                raise MinioFailureException()
            if policy_delete_failure and args[0:4] == ('admin', 'policy', 'rm', 'cwm'):
                raise MinioFailureException()
            if policy_attach_failure and args[0:4] == ('admin', 'policy', 'attach', 'cwm'):
                raise MinioFailureException()
            if policy_detach_failure and args[0:4] == ('admin', 'policy', 'detach', 'cwm'):
                raise MinioFailureException()
            if anon_set_download_failure and args[0:3] == ('anonymous', 'set', 'download'):
                raise MinioFailureException()
            if anon_set_none_failure and args[0:3] == ('anonymous', 'set', 'none'):
                raise MinioFailureException()
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    with pytest.raises(Exception) as e:
        await buckets_api.create(instance_id, bucket_name, public=public)
    if isinstance(e.value, MinioFailureException):
        pass
    elif isinstance(e.value, ExceptionGroup):
        assert all(isinstance(ex, MinioFailureException) for ex in e.value.exceptions)
    else:
        raise e
    expected = [('mc_check_call', 'mb')]
    if not mb_failure:
        if public:
            expected += [('mc_check_call', 'anonymous', 'set', 'download')]
        if not anon_set_download_failure:
            expected += [
                ('mc_check_call', 'admin', 'policy', 'create', 'cwm'),
                ('mc_check_call', 'admin', 'policy', 'create', 'cwm'),
                ('mc_check_call', 'admin', 'policy', 'create', 'cwm'),
            ]
            if not policy_create_failure:
                assert policy_attach_failure
                expected += [
                    ('mc_check_call', 'admin', 'policy', 'attach', 'cwm'),
                    ('mc_check_call', 'admin', 'policy', 'attach', 'cwm'),
                    ('mc_check_call', 'admin', 'policy', 'attach', 'cwm'),
                    ('mc_check_call', 'admin', 'policy', 'detach', 'cwm'),
                    ('mc_check_call', 'admin', 'policy', 'detach', 'cwm'),
                    ('mc_check_call', 'admin', 'policy', 'detach', 'cwm'),
                ]
            expected += [
                ('mc_check_call', 'admin', 'policy', 'rm', 'cwm'),
                ('mc_check_call', 'admin', 'policy', 'rm', 'cwm'),
                ('mc_check_call', 'admin', 'policy', 'rm', 'cwm'),
            ]
        if public:
            expected += [('mc_check_call', 'anonymous', 'set', 'none')]
    expected += [('mc_check_call', 'rb', 'cwm/bucket', '--force')]
    cwm_test_db['tracker_assert_calls'](expected)
    assert [b async for b in buckets_api.list_iterator(instance_id)] == []
