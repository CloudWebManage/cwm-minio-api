from cwm_minio_api.instances import api as instances_api
from cwm_minio_api.buckets import api as buckets_api
from cwm_minio_api import common, config
from cwm_minio_api.minio import api as minio_api


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
