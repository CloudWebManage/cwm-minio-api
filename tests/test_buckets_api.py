from cwm_minio_api.instances import api as instances_api
from cwm_minio_api.buckets import api as buckets_api


async def test_crud(cwm_test_db):
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    await instances_api.create(instance_id)
    created_bucket = await buckets_api.create(instance_id, bucket_name)
    assert [bucket_name async for bucket_name in buckets_api.list_iterator(instance_id)] == [bucket_name]
    bucket = await buckets_api.get(instance_id, bucket_name)
    assert bucket.keys() == {'blocked', 'bucket_name', 'instance_id', 'public'}
    assert bucket['blocked'] is False
    assert bucket['bucket_name'] == bucket_name
    assert bucket['instance_id'] == instance_id
    assert bucket['public'] is False
    assert created_bucket == bucket
    updated_bucket = await buckets_api.update(instance_id, bucket_name, blocked=True, public=True)
    assert updated_bucket == {
        **bucket,
        'blocked': True,
        'public': True
    }
    assert updated_bucket == await buckets_api.get(instance_id, bucket_name)
    await buckets_api.delete(instance_id, bucket_name)
    assert [bucket_name async for bucket_name in buckets_api.list_iterator(instance_id)] == []
    assert await buckets_api.get(instance_id, bucket_name) is None
