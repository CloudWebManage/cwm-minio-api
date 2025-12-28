import pytest

from cwm_minio_api.instances import api as instances_api
from cwm_minio_api.buckets import api as buckets_api
from cwm_minio_api import db


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


async def test_instance_delete_without_accesskey(cwm_test_db):
    instance_id = 'test_instance_1'
    await instances_api.create(instance_id)
    async with db.connection_cursor() as (conn, cur):
        await cur.execute('UPDATE instances SET access_key = NULL WHERE id = %s', (instance_id,))
        await conn.commit()
    await instances_api.delete(instance_id)


async def test_instance_lock_unlock(cwm_test_db):
    tw = cwm_test_db["tracker_get_calls"]

    def bucket_policy_arg(t, bucket_name):
        return 'FILE::' + getattr(buckets_api, f'BUCKET_POLICY_{t.upper()}_TEMPLATE').replace("__BUCKET_NAME__", bucket_name)

    instance_id = 'test_instance'
    instance = await instances_api.create(instance_id)
    assert instance['blocked'] is False
    assert instance['num_buckets'] == 0
    access_key = instance['access_key']
    secret_key = instance['secret_key']
    assert tw() == [
        ("mc_check_call", ('admin', 'user', 'add', 'cwm', access_key, secret_key)),
    ]
    private_bucket_name = 'private'
    public_bucket_name = 'public'
    await buckets_api.create(instance_id, private_bucket_name, public=False)
    assert tw() == [
        ("mc_check_call", ('mb', f'cwm/{private_bucket_name}')),
        ("mc_check_call", ('admin', 'policy', 'create', 'cwm', f'{private_bucket_name}_read', bucket_policy_arg('read', private_bucket_name))),
        ("mc_check_call", ('admin', 'policy', 'create', 'cwm', f'{private_bucket_name}_write', bucket_policy_arg('write', private_bucket_name))),
        ("mc_check_call", ('admin', 'policy', 'create', 'cwm', f'{private_bucket_name}_delete', bucket_policy_arg('delete', private_bucket_name))),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{private_bucket_name}_read', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{private_bucket_name}_write', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{private_bucket_name}_delete', '--user', access_key)),
    ]
    await buckets_api.create(instance_id, public_bucket_name, public=True)
    assert tw() == [
        ("mc_check_call", ('mb', f'cwm/{public_bucket_name}')),
        ("mc_check_call", ('anonymous', "set", 'download', f'cwm/{public_bucket_name}')),
        ("mc_check_call", ('admin', 'policy', 'create', 'cwm', f'{public_bucket_name}_read', bucket_policy_arg('read', public_bucket_name))),
        ("mc_check_call", ('admin', 'policy', 'create', 'cwm', f'{public_bucket_name}_write', bucket_policy_arg('write', public_bucket_name))),
        ("mc_check_call", ('admin', 'policy', 'create', 'cwm', f'{public_bucket_name}_delete', bucket_policy_arg('delete', public_bucket_name))),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{public_bucket_name}_read', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{public_bucket_name}_write', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{public_bucket_name}_delete', '--user', access_key)),
    ]
    private_read_creds = await buckets_api.credentials_create(instance_id, private_bucket_name, read=True, write=False, delete=False)
    assert tw() == [
        ("mc_check_call", ('admin', 'user', 'add', 'cwm', private_read_creds['access_key'], private_read_creds['secret_key'])),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{private_bucket_name}_read', '--user', private_read_creds['access_key'])),
    ]
    private_write_creds = await buckets_api.credentials_create(instance_id, private_bucket_name, read=False, write=True, delete=False)
    assert tw() == [
        ("mc_check_call", ('admin', 'user', 'add', 'cwm', private_write_creds['access_key'], private_write_creds['secret_key'])),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{private_bucket_name}_write', '--user', private_write_creds['access_key'])),
    ]
    public_write_creds = await buckets_api.credentials_create(instance_id, public_bucket_name, read=False, write=True, delete=False)
    assert tw() == [
        ("mc_check_call", ('admin', 'user', 'add', 'cwm', public_write_creds['access_key'], public_write_creds['secret_key'])),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{public_bucket_name}_write', '--user', public_write_creds['access_key'])),
    ]
    assert {c['access_key']: c async for c in buckets_api.credentials_list_iterator(instance_id, private_bucket_name)} == {
        private_read_creds['access_key']: {
            'access_key': private_read_creds['access_key'],
            'permission_read': True,
            'permission_write': False,
            'permission_delete': False,
        },
        private_write_creds['access_key']: {
            'access_key': private_write_creds['access_key'],
            'permission_read': False,
            'permission_write': True,
            'permission_delete': False,
        }
    }
    assert {c['access_key']: c async for c in buckets_api.credentials_list_iterator(instance_id, public_bucket_name)} == {
        public_write_creds['access_key']: {
            'access_key': public_write_creds['access_key'],
            'permission_read': False,
            'permission_write': True,
            'permission_delete': False,
        }
    }
    assert tw() == []
    await instances_api.update(instance_id, blocked=True)
    assert set(tw()) == {
        # remove public download access
        ("mc_check_call", ('anonymous', 'set', 'none', f'cwm/{public_bucket_name}')),

        ## Private bucket ##
        # Detach policies from main user
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{private_bucket_name}_read', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{private_bucket_name}_write', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{private_bucket_name}_delete', '--user', access_key)),

        ## Public bucket ##
        # Detach policies from main user
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{public_bucket_name}_read', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{public_bucket_name}_write', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{public_bucket_name}_delete', '--user', access_key)),

        ## Detach credentials ##
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{private_bucket_name}_read', '--user', private_read_creds['access_key'])),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{private_bucket_name}_write', '--user', private_read_creds['access_key'])),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{private_bucket_name}_delete', '--user', private_read_creds['access_key'])),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{private_bucket_name}_read', '--user', private_write_creds['access_key'])),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{private_bucket_name}_write', '--user', private_write_creds['access_key'])),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{private_bucket_name}_delete', '--user', private_write_creds['access_key'])),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{public_bucket_name}_read', '--user', public_write_creds['access_key'])),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{public_bucket_name}_write', '--user', public_write_creds['access_key'])),
        ("mc_check_call", ('admin', 'policy', 'detach', 'cwm', f'{public_bucket_name}_delete', '--user', public_write_creds['access_key'])),
    }
    await instances_api.update(instance_id, blocked=False)
    assert set(tw()) == {
        # return public download access only for public bucket
        ("mc_check_call", ('anonymous', "set", 'download', f'cwm/{public_bucket_name}')),

        # Re-attach policies to main user
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{public_bucket_name}_read', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{public_bucket_name}_write', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{public_bucket_name}_delete', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{private_bucket_name}_read', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{private_bucket_name}_write', '--user', access_key)),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{private_bucket_name}_delete', '--user', access_key)),

        # Re-attach credentials policies
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{public_bucket_name}_write', '--user', public_write_creds['access_key'])),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{private_bucket_name}_read', '--user', private_read_creds['access_key'])),
        ("mc_check_call", ('admin', 'policy', 'attach', 'cwm', f'{private_bucket_name}_write', '--user', private_write_creds['access_key'])),
    }
