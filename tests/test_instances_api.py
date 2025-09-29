import pytest

from cwm_minio_api.instances import api as instances_api
from cwm_minio_api.buckets import api as buckets_api


async def test_crud(cwm_test_db):
    instance_id = 'test_instance_1'
    created_instance = await instances_api.create(instance_id)
    assert created_instance.keys() == {'instance_id', 'blocked', 'num_buckets', 'access_key', 'secret_key'}
    assert created_instance['instance_id'] == instance_id
    assert created_instance['blocked'] is False
    assert created_instance['num_buckets'] == 0
    assert len(created_instance['access_key']) == 24
    secret_key = created_instance.pop('secret_key')
    assert len(secret_key) == 40
    assert [i async for i in instances_api.list_iterator()] == [instance_id]
    assert (await instances_api.get(instance_id)) == created_instance
    updated_instance = await instances_api.update(instance_id, blocked=True)
    assert updated_instance == {
        **created_instance,
        'blocked': True
    }
    bucket_name = 'test-bucket-1'
    with pytest.raises(Exception, match='Instance is blocked'):
        await buckets_api.create(instance_id, bucket_name)
    await instances_api.update(instance_id, blocked=False)
    await buckets_api.create(instance_id, bucket_name)
    assert (await instances_api.get(instance_id)) == {
        **updated_instance,
        'blocked': False,
        'num_buckets': 1
    }
    bucket_access_key = (await buckets_api.credentials_create(instance_id, bucket_name, read=True, write=True, delete=True))['access_key']
    assert [c['access_key'] async for c in buckets_api.credentials_list_iterator(instance_id, bucket_name)] == [bucket_access_key]
    await instances_api.delete(instance_id)
    assert [i async for i in instances_api.list_iterator()] == []
