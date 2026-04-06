import pytest

from cwm_minio_api.instances import api as instances_api
from cwm_minio_api.buckets import api as buckets_api
from cwm_minio_api import common, config
from cwm_minio_api.minio import api as minio_api
from cwm_minio_api.credentials import api as credentials_api


async def test_crud(cwm_test_db):
    tw = cwm_test_db["tracker_get_calls"]
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    instance = await instances_api.create(instance_id)
    access_key = instance['access_key']
    secret_key = instance['secret_key']
    assert tw() == [
        ("mc_check_call", ('admin', 'user', 'add', 'cwm', access_key, secret_key)),
    ]
    created_bucket = await buckets_api.create(instance_id, bucket_name)
    assert tw() == [
        ("mc_check_call", ('mb', f'cwm/{bucket_name}')),
        ("mc_check_call", ('admin', 'policy', 'create', 'cwm', f'{bucket_name}_read', cwm_test_db['get_bucket_policy_arg']('read', bucket_name))),
        ("mc_check_call", ('admin', 'policy', 'create', 'cwm', f'{bucket_name}_write', cwm_test_db['get_bucket_policy_arg']('write', bucket_name))),
        ("mc_check_call", ('admin', 'policy', 'create', 'cwm', f'{bucket_name}_delete', cwm_test_db['get_bucket_policy_arg']('delete', bucket_name))),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{bucket_name}_read', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{bucket_name}_write', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{bucket_name}_delete', '--user', access_key)),
    ]
    assert [bucket_name async for bucket_name in buckets_api.list_iterator(instance_id)] == [bucket_name]
    bucket = await buckets_api.get(instance_id, bucket_name)
    assert bucket.keys() == {'blocked', 'bucket_name', 'instance_id', 'public'}
    assert bucket['blocked'] is False
    assert bucket['bucket_name'] == bucket_name
    assert bucket['instance_id'] == instance_id
    assert bucket['public'] is False
    assert created_bucket == bucket
    updated_bucket = await buckets_api.update(instance_id, bucket_name, blocked=True, public=True)
    assert tw() == [
        ('mc_check_call', ('anonymous', 'set', 'download', f'cwm/{bucket_name}')),
        ('mc_check_call', ('anonymous', 'set', 'none', f'cwm/{bucket_name}')),
        *[
            ('mc_check_call', ('admin', 'policy', 'detach', 'cwm', f'{bucket_name}_{p}', '--user', access_key))
            for p in ['read', 'write', 'delete']
        ]
    ]
    assert updated_bucket == {
        **bucket,
        'blocked': True,
        'public': True
    }
    assert updated_bucket == await buckets_api.get(instance_id, bucket_name)
    with pytest.raises(Exception, match="Bucket is blocked"):
        await buckets_api.credentials_create(instance_id, bucket_name, "", True, False, True)
    await buckets_api.update(instance_id, bucket_name, blocked=False, public=False)
    assert tw() == [
        ('mc_check_call', ('anonymous', 'set', 'none', f'cwm/{bucket_name}')),
        *[
            ('mc_check_call', ('admin', 'policy', 'attach', 'cwm', f'{bucket_name}_{p}', '--user', access_key))
            for p in ['read', 'write', 'delete']
        ]
    ]
    with pytest.raises(Exception, match="Credentials not found"):
        await buckets_api.credentials_create(instance_id, bucket_name, "", True, False, True)
    credentials = await credentials_api.create(instance_id)
    credentials_access_key = credentials['access_key']
    assert tw() == [
        ('mc_check_call', ('admin', 'user', 'add', 'cwm', credentials_access_key, credentials['secret_key']))
    ]
    bucket_credentials = await buckets_api.credentials_create(instance_id, bucket_name, credentials_access_key, True, False, True)
    assert tw() == [
        ('mc_check_call', ('admin', 'policy', 'attach', 'cwm', f'{bucket_name}_{p}', '--user', credentials_access_key))
        for p in ['read', 'delete']
    ]
    assert bucket_credentials == {
        'access_key': credentials_access_key,
        'permission_read': True,
        'permission_write': False,
        'permission_delete': True,
    }
    await buckets_api.delete(instance_id, bucket_name)
    assert tw() == [
        *[
            ('mc_check_call', ('admin', 'policy', 'detach', 'cwm', f'{bucket_name}_{p}', '--user', access_key))
            for p in ['read', 'write', 'delete']
        ],
        *[
            ('mc_check_call', ('admin', 'policy', 'detach', 'cwm', f'{bucket_name}_{p}', '--user', credentials_access_key))
            for p in ['read', 'write', 'delete']
        ],
        *[
            ('mc_check_call', ('admin', 'policy', 'rm', 'cwm', f'{bucket_name}_{p}'))
            for p in ['read', 'write', 'delete']
        ],
        ('mc_check_call', ('rb', f'cwm/{bucket_name}', '--force')),
    ]
    assert [bucket_name async for bucket_name in buckets_api.list_iterator(instance_id)] == []
    assert await buckets_api.get(instance_id, bucket_name) is None


async def test_bucket_get_size(cwm_test_minio):
    profile, prefix = cwm_test_minio
    instance_id = 'test_instance_1'
    bucket_with_objects = f'{prefix}-bucket-with-objects'
    empty_bucket = f'{prefix}-empty-bucket'
    invalid_bucket = f'{prefix}-invalid-bucket'
    await instances_api.create(instance_id)
    await buckets_api.create(instance_id, bucket_with_objects)
    await buckets_api.create(instance_id, empty_bucket)
    await buckets_api.create(instance_id, invalid_bucket)
    await common.async_subprocess_check_call(config.MINIO_MC_BINARY, 'cp', '-r', 'cwm_minio_api', f'{profile}/{bucket_with_objects}/')

    async def bucket_has_size():
        return (await buckets_api.get(instance_id, bucket_with_objects, with_size=True))['size_bytes'] > 0

    await common.wait_for(bucket_has_size, 60, 1)
    res = await buckets_api.get(instance_id, bucket_with_objects, with_size=True)
    assert res['size_bytes'] > 100
    res = await buckets_api.get(instance_id, empty_bucket, with_size=True)
    assert res['size_bytes'] == 0
    await common.async_subprocess_check_call(config.MINIO_MC_BINARY, 'rb', f'{profile}/{invalid_bucket}', '--force')
    res = await buckets_api.get(instance_id, invalid_bucket, with_size=True)
    assert res['size_bytes'] is None
    async for b in buckets_api.list_iterator(instance_id, with_size=True):
        if b['name'] == bucket_with_objects:
            assert b['size_bytes'] > 100
        elif b['name'] == empty_bucket:
            assert b['size_bytes'] == 0
        elif b['name'] == invalid_bucket:
            assert b['size_bytes'] is None
        elif b['name'] == '*':
            assert b['size_bytes'] > 100
        else:
            raise AssertionError(f'Unexpected bucket name: {b["name"]}')


async def test_bucket_create_minio_exception(cwm_test_db, monkeypatch):
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    await instances_api.create(instance_id)
    monkeypatch.setattr('cwm_minio_api.config.MINIO_MC_BINARY', '__INVALID__')
    try:
        await buckets_api.create(instance_id, bucket_name)
    except Exception as e:
        assert str(e) == "[Errno 2] No such file or directory: '__INVALID__'"
    else:
        raise AssertionError('Expected exception was not raised')
    monkeypatch.setattr('cwm_minio_api.config.MINIO_MC_BINARY', 'bash')
    try:
        await minio_api.mc_check_call("-c", "echo simulated error >&2; exit 1")
    except Exception as e:
        assert str(e) == 'simulated error'
    else:
        raise AssertionError('Expected exception was not raised')
