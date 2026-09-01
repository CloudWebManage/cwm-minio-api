import pytest
import orjson
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from cwm_minio_api.app import app
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
        ("mc_check_call", (
            'ilm', 'rule', 'add',
            '--tags', 'cwm-tier=low',
            '--transition-days', '0',
            '--transition-tier', 'LOW',
            f'cwm/{bucket_name}',
        )),
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


async def test_bucket_create_lifecycle_failure_rolls_back(cwm_test_db):
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    await instances_api.create(instance_id)
    cwm_test_db['tracker_get_calls']()

    async def intercept(f, name, *args):
        if name == 'mc_check_call' and args[:3] == ('ilm', 'rule', 'add'):
            raise AssertionError('unable to add transition rule')
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    with pytest.raises(AssertionError, match='unable to add transition rule'):
        await buckets_api.create(instance_id, bucket_name)

    assert cwm_test_db['tracker_get_calls']() == [
        ('mc_check_call', ('mb', f'cwm/{bucket_name}')),
        ('mc_check_call', (
            'ilm', 'rule', 'add',
            '--tags', 'cwm-tier=low',
            '--transition-days', '0',
            '--transition-tier', 'LOW',
            f'cwm/{bucket_name}',
        )),
        ('mc_check_call', ('rb', f'cwm/{bucket_name}', '--force')),
    ]
    assert await buckets_api.get(instance_id, bucket_name) is None


async def test_update_versioning_defaults(cwm_test_db):
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    await instances_api.create(instance_id)
    await buckets_api.create(instance_id, bucket_name)
    cwm_test_db['tracker_get_calls']()

    async def intercept(f, name, *args):
        if name == 'mc_check_output' and args[:3] == ('ilm', 'rule', 'add'):
            return orjson.dumps({
                'status': 'success',
                'target': f'cwm/{bucket_name}',
                'id': 'default-rule-id',
            }).decode()
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    assert await buckets_api.update_versioning(instance_id, bucket_name) == {
        'instance_id': instance_id,
        'bucket_name': bucket_name,
        'enabled': False,
        'expire_delete_marker': True,
        'noncurrent_expire_days': None,
        'noncurrent_expire_newer': None,
    }
    assert cwm_test_db['tracker_get_calls']() == [
        ('mc_check_output', ('ilm', 'rule', 'add', '--expire-delete-marker', f'cwm/{bucket_name}', '--json')),
        ('mc_check_call', ('version', 'suspend', f'cwm/{bucket_name}')),
    ]


async def test_update_versioning_replaces_managed_rule(cwm_test_db):
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    await instances_api.create(instance_id)
    await buckets_api.create(instance_id, bucket_name)
    cwm_test_db['tracker_get_calls']()
    rule_ids = iter(('first-rule-id', 'replacement-rule-id'))

    async def intercept(f, name, *args):
        if name == 'mc_check_output' and args[:3] == ('ilm', 'rule', 'add'):
            return orjson.dumps({
                'status': 'success',
                'target': f'cwm/{bucket_name}',
                'id': next(rule_ids),
            }).decode()
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    await buckets_api.update_versioning(instance_id, bucket_name)
    cwm_test_db['tracker_get_calls']()

    assert await buckets_api.update_versioning(
        instance_id,
        bucket_name,
        enabled=True,
        expire_delete_marker=False,
        noncurrent_expire_days=30,
        noncurrent_expire_newer=2,
    ) == {
        'instance_id': instance_id,
        'bucket_name': bucket_name,
        'enabled': True,
        'expire_delete_marker': False,
        'noncurrent_expire_days': 30,
        'noncurrent_expire_newer': 2,
    }
    assert cwm_test_db['tracker_get_calls']() == [
        ('mc_check_output', (
            'ilm', 'rule', 'add',
            '--noncurrent-expire-days', '30',
            '--noncurrent-expire-newer', '2',
            f'cwm/{bucket_name}', '--json',
        )),
        ('mc_check_call', ('version', 'enable', f'cwm/{bucket_name}')),
        ('mc_check_call', ('ilm', 'rule', 'remove', '--id', 'first-rule-id', f'cwm/{bucket_name}', '--json')),
    ]

    await buckets_api.update_versioning(
        instance_id,
        bucket_name,
        enabled=False,
        expire_delete_marker=False,
    )
    assert cwm_test_db['tracker_get_calls']() == [
        ('mc_check_call', ('version', 'suspend', f'cwm/{bucket_name}')),
        ('mc_check_call', ('ilm', 'rule', 'remove', '--id', 'replacement-rule-id', f'cwm/{bucket_name}', '--json')),
    ]


async def test_versioning_lifecycle_failure_does_not_change_versioning(cwm_test_db):
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    await instances_api.create(instance_id)
    await buckets_api.create(instance_id, bucket_name)
    cwm_test_db['tracker_get_calls']()

    async def intercept(f, name, *args):
        if name == 'mc_check_output' and args[:3] == ('ilm', 'rule', 'add'):
            raise AssertionError('unable to add lifecycle rule')
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    with pytest.raises(AssertionError, match='unable to add lifecycle rule'):
        await buckets_api.update_versioning(instance_id, bucket_name, enabled=True)
    assert cwm_test_db['tracker_get_calls']() == [
        ('mc_check_output', ('ilm', 'rule', 'add', '--expire-delete-marker', f'cwm/{bucket_name}', '--json')),
    ]


async def test_versioning_restores_previous_state_when_rule_replacement_fails(cwm_test_db):
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    await instances_api.create(instance_id)
    await buckets_api.create(instance_id, bucket_name)
    cwm_test_db['tracker_get_calls']()
    rule_ids = iter(('first-rule-id', 'replacement-rule-id'))

    async def intercept(f, name, *args):
        if name == 'mc_check_output' and args[:3] == ('ilm', 'rule', 'add'):
            return orjson.dumps({'id': next(rule_ids)}).decode()
        if name == 'mc_check_call' and args[:5] == ('ilm', 'rule', 'remove', '--id', 'first-rule-id'):
            raise AssertionError('permission denied')
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    await buckets_api.update_versioning(instance_id, bucket_name)
    cwm_test_db['tracker_get_calls']()

    with pytest.raises(AssertionError, match='permission denied'):
        await buckets_api.update_versioning(instance_id, bucket_name, enabled=True)
    assert cwm_test_db['tracker_get_calls']() == [
        ('mc_check_output', ('ilm', 'rule', 'add', '--expire-delete-marker', f'cwm/{bucket_name}', '--json')),
        ('mc_check_call', ('version', 'enable', f'cwm/{bucket_name}')),
        ('mc_check_call', ('ilm', 'rule', 'remove', '--id', 'first-rule-id', f'cwm/{bucket_name}', '--json')),
        ('mc_check_call', ('version', 'suspend', f'cwm/{bucket_name}')),
        ('mc_check_call', ('ilm', 'rule', 'remove', '--id', 'replacement-rule-id', f'cwm/{bucket_name}', '--json')),
    ]


async def test_versioning_restores_previous_state_when_version_command_fails(cwm_test_db):
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    await instances_api.create(instance_id)
    await buckets_api.create(instance_id, bucket_name)
    cwm_test_db['tracker_get_calls']()
    version_calls = 0

    async def intercept(f, name, *args):
        nonlocal version_calls
        if name == 'mc_check_output' and args[:3] == ('ilm', 'rule', 'add'):
            return orjson.dumps({'id': 'new-rule-id'}).decode()
        if name == 'mc_check_call' and args[:2] == ('version', 'enable'):
            version_calls += 1
            raise TimeoutError('response lost after MinIO applied the change')
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    with pytest.raises(TimeoutError, match='response lost'):
        await buckets_api.update_versioning(instance_id, bucket_name, enabled=True)
    assert version_calls == 1
    assert cwm_test_db['tracker_get_calls']() == [
        ('mc_check_output', ('ilm', 'rule', 'add', '--expire-delete-marker', f'cwm/{bucket_name}', '--json')),
        ('mc_check_call', ('version', 'enable', f'cwm/{bucket_name}')),
        ('mc_check_call', ('version', 'suspend', f'cwm/{bucket_name}')),
        ('mc_check_call', ('ilm', 'rule', 'remove', '--id', 'new-rule-id', f'cwm/{bucket_name}', '--json')),
    ]


@pytest.mark.parametrize('message', (
    '{"status":"error","error":{"message":"Unable to remove rule by id","cause":{"message":"lifecycle rule for id \'stale-rule-id\' not found","error":{}},"type":"fatal"}}',
    '{"status":"error","error":{"message":"Unable to fetch lifecycle rules","cause":{"message":"The lifecycle configuration does not exist","error":{"Code":"NoSuchLifecycleConfiguration"}},"type":"fatal"}}',
))
async def test_remove_missing_managed_lifecycle_rule_is_idempotent(cwm_test_db, message):
    async def intercept(f, name, *args):
        if name == 'mc_check_call' and args[:3] == ('ilm', 'rule', 'remove'):
            raise AssertionError(message)
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    await minio_api.remove_bucket_lifecycle_rule('test-bucket-1', 'stale-rule-id')


async def test_remove_managed_lifecycle_rule_preserves_other_errors(cwm_test_db):
    async def intercept(f, name, *args):
        if name == 'mc_check_call' and args[:3] == ('ilm', 'rule', 'remove'):
            raise AssertionError('unable to remove lifecycle rule for id due to permission denied')
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    with pytest.raises(AssertionError, match='permission denied'):
        await minio_api.remove_bucket_lifecycle_rule('test-bucket-1', 'managed-rule-id')


def test_versioning_request_accepts_documented_option_names():
    from cwm_minio_api.buckets.router import VersioningRequest

    request = VersioningRequest.model_validate({
        'instance_id': 'test_instance_1',
        'bucket_name': 'test-bucket-1',
        'expire-delete-marker': False,
        'noncurrent-expire-days': 30,
        'noncurrent-expire-newer': 2,
    })
    assert request.enabled is False
    assert request.expire_delete_marker is False
    assert request.noncurrent_expire_days == 30
    assert request.noncurrent_expire_newer == 2


@pytest.mark.parametrize('field,value', (
    ('noncurrent-expire-days', 0),
    ('noncurrent-expire-newer', -1),
))
def test_versioning_request_rejects_invalid_lifecycle_values(field, value):
    from cwm_minio_api.buckets.router import VersioningRequest

    with pytest.raises(ValidationError):
        VersioningRequest.model_validate({
            'instance_id': 'test_instance_1',
            'bucket_name': 'test-bucket-1',
            field: value,
        })


async def test_update_versioning_http_endpoint(cwm_test_db):
    instance_id = 'test_instance_1'
    bucket_name = 'test-bucket-1'
    await instances_api.create(instance_id)
    await buckets_api.create(instance_id, bucket_name)
    cwm_test_db['tracker_get_calls']()

    async def intercept(f, name, *args):
        if name == 'mc_check_output' and args[:3] == ('ilm', 'rule', 'add'):
            return orjson.dumps({'id': 'http-rule-id'}).decode()
        return await f(*args)

    cwm_test_db['intercept'] = intercept
    async with AsyncClient(transport=ASGITransport(app=app()), base_url='http://test') as client:
        response = await client.put('/buckets/versioning', json={
            'instance_id': instance_id,
            'bucket_name': bucket_name,
            'enabled': True,
            'expire-delete-marker': False,
            'noncurrent-expire-days': 30,
            'noncurrent-expire-newer': 2,
        })
    assert response.status_code == 200
    assert response.json() == {
        'instance_id': instance_id,
        'bucket_name': bucket_name,
        'enabled': True,
        'expire_delete_marker': False,
        'noncurrent_expire_days': 30,
        'noncurrent_expire_newer': 2,
    }


async def test_bucket_versioning_minio(cwm_test_minio):
    profile, prefix = cwm_test_minio
    instance_id = f'{prefix}-instance'
    bucket_name = f'{prefix}-versioning'
    await instances_api.create(instance_id)
    await buckets_api.create(instance_id, bucket_name)

    try:
        unrelated_rule_id = await minio_api.add_bucket_lifecycle_rule(
            bucket_name,
            expire_delete_marker=False,
            noncurrent_expire_days=90,
        )
        await buckets_api.update_versioning(
            instance_id,
            bucket_name,
            enabled=True,
            expire_delete_marker=True,
            noncurrent_expire_days=30,
            noncurrent_expire_newer=2,
        )
        versioning = orjson.loads(await minio_api.mc_check_output(
            'version', 'info', f'{profile}/{bucket_name}', '--json',
        ))
        assert versioning['versioning']['status'] == 'Enabled'

        lifecycle = orjson.loads(await minio_api.mc_check_output(
            'ilm', 'rule', 'list', f'{profile}/{bucket_name}', '--json',
        ))
        rules = {rule['ID']: rule for rule in lifecycle['config']['Rules']}
        assert rules.pop(unrelated_rule_id) == {
            'ID': unrelated_rule_id,
            'NoncurrentVersionExpiration': {'NoncurrentDays': 90},
            'Status': 'Enabled',
        }
        assert len(rules) == 1
        managed_rule = next(iter(rules.values()))
        assert managed_rule.pop('ID')
        assert managed_rule == {
            'Expiration': {'ExpiredObjectDeleteMarker': True},
            'NoncurrentVersionExpiration': {
                'NoncurrentDays': 30,
                'NewerNoncurrentVersions': 2,
            },
            'Status': 'Enabled',
        }

        await buckets_api.update_versioning(
            instance_id,
            bucket_name,
            enabled=False,
            expire_delete_marker=False,
        )
        versioning = orjson.loads(await minio_api.mc_check_output(
            'version', 'info', f'{profile}/{bucket_name}', '--json',
        ))
        assert versioning['versioning']['status'] == 'Suspended'
        lifecycle = orjson.loads(await minio_api.mc_check_output(
            'ilm', 'rule', 'list', f'{profile}/{bucket_name}', '--json',
        ))
        assert lifecycle['config']['Rules'] == [{
            'ID': unrelated_rule_id,
            'NoncurrentVersionExpiration': {'NoncurrentDays': 90},
            'Status': 'Enabled',
        }]
    finally:
        await buckets_api.delete(instance_id, bucket_name)
