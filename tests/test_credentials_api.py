import pytest

from cwm_minio_api.buckets import api as buckets_api
from cwm_minio_api.credentials import api as credentials_api
from cwm_minio_api.instances import api as instances_api


async def test_crud(cwm_test_db):
    tracker_get_calls = cwm_test_db['tracker_get_calls']
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    await instances_api.create(instance_id)
    tracker_get_calls()

    created_credentials = await credentials_api.create(instance_id)
    assert created_credentials.keys() == {'access_key', 'secret_key'}
    assert len(created_credentials['access_key']) == 24
    assert len(created_credentials['secret_key']) == 40
    assert [credential async for credential in credentials_api.list_iterator(instance_id)] == [
        {'access_key': created_credentials['access_key']},
    ]
    assert tracker_get_calls() == [
        ('mc_check_call', ('admin', 'user', 'add', 'cwm', created_credentials['access_key'], created_credentials['secret_key'])),
    ]

    await buckets_api.create(instance_id, bucket_name)
    tracker_get_calls()

    assigned_credentials = await buckets_api.credentials_create(
        instance_id,
        bucket_name,
        created_credentials['access_key'],
        read=True,
        write=False,
        delete=False,
    )
    assert assigned_credentials == {
        'access_key': created_credentials['access_key'],
        'permission_read': True,
        'permission_write': False,
        'permission_delete': False,
    }
    assert [credential async for credential in buckets_api.credentials_list_iterator(instance_id, bucket_name)] == [
        assigned_credentials,
    ]
    assert tracker_get_calls() == [
        ('mc_check_call', ('admin', 'policy', 'attach', 'cwm', f'{bucket_name}_read', '--user', created_credentials['access_key'])),
    ]

    with pytest.raises(Exception, match='Credentials are assigned to buckets'):
        await credentials_api.delete(created_credentials['access_key'])
    assert tracker_get_calls() == []

    updated_credentials = await buckets_api.credentials_update(
        instance_id,
        bucket_name,
        created_credentials['access_key'],
        read=False,
        write=True,
        delete=True,
    )
    assert updated_credentials == {
        'access_key': created_credentials['access_key'],
        'permission_read': False,
        'permission_write': True,
        'permission_delete': True,
    }
    assert set(tracker_get_calls()) == {
        ('mc_check_call', ('admin', 'policy', 'detach', 'cwm', f'{bucket_name}_read', '--user', created_credentials['access_key'])),
        ('mc_check_call', ('admin', 'policy', 'detach', 'cwm', f'{bucket_name}_write', '--user', created_credentials['access_key'])),
        ('mc_check_call', ('admin', 'policy', 'detach', 'cwm', f'{bucket_name}_delete', '--user', created_credentials['access_key'])),
        ('mc_check_call', ('admin', 'policy', 'attach', 'cwm', f'{bucket_name}_write', '--user', created_credentials['access_key'])),
        ('mc_check_call', ('admin', 'policy', 'attach', 'cwm', f'{bucket_name}_delete', '--user', created_credentials['access_key'])),
    }

    await buckets_api.credentials_delete(instance_id, bucket_name, created_credentials['access_key'])
    assert set(tracker_get_calls()) == {
        ('mc_check_call', ('admin', 'policy', 'detach', 'cwm', f'{bucket_name}_read', '--user', created_credentials['access_key'])),
        ('mc_check_call', ('admin', 'policy', 'detach', 'cwm', f'{bucket_name}_write', '--user', created_credentials['access_key'])),
        ('mc_check_call', ('admin', 'policy', 'detach', 'cwm', f'{bucket_name}_delete', '--user', created_credentials['access_key'])),
    }

    await credentials_api.delete(created_credentials['access_key'])
    assert [credential async for credential in credentials_api.list_iterator(instance_id)] == []
    assert tracker_get_calls() == [
        ('mc_check_call', ('admin', 'user', 'rm', 'cwm', created_credentials['access_key'])),
    ]
