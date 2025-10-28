import traceback
from textwrap import dedent
from contextlib import AsyncExitStack

from .. import db, common, access_keys
from ..instances.api import get as get_instance
from ..minio import api as minio_api


# https://docs.min.io/community/minio-object-store/administration/identity-access-management/policy-based-access-control.html
BUCKET_POLICY_READ_TEMPLATE = dedent('''
{
   "Version" : "2012-10-17",
   "Statement" : [
      {
         "Effect" : "Allow",
         "Action" : [
            "s3:GetBucketLocation",
            "s3:ListAllMyBuckets",
            "s3:GetObject",
            "s3:GetObjectAttributes",
            "s3:GetObjectVersionAttributes",
            "s3:ListBucket",
            "s3:GetObjectTagging"
        ],
         "Resource": [
            "arn:aws:s3:::__BUCKET_NAME__",
            "arn:aws:s3:::__BUCKET_NAME__/*"
        ]
      }
   ]
}
''')
BUCKET_POLICY_WRITE_TEMPLATE = dedent('''
{
   "Version" : "2012-10-17",
   "Statement" : [
      {
         "Effect" : "Allow",
         "Action" : [
            "s3:RestoreObject",
            "s3:PutObject",
            "s3:PutObjectTagging",
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts",
            "s3:ListBucketMultipartUploads"
        ],
         "Resource": [
            "arn:aws:s3:::__BUCKET_NAME__",
            "arn:aws:s3:::__BUCKET_NAME__/*"
        ]
      }
   ]
}
''')
BUCKET_POLICY_DELETE_TEMPLATE = dedent('''
{
   "Version" : "2012-10-17",
   "Statement" : [
      {
         "Effect" : "Allow",
         "Action" : [
            "s3:DeleteObject",
            "s3:DeleteObjectTagging"
        ],
         "Resource": [
            "arn:aws:s3:::__BUCKET_NAME__",
            "arn:aws:s3:::__BUCKET_NAME__/*"
        ]
      }
   ]
}
''')


async def create(instance_id, bucket_name, public=False):
    common.check_bucket_name(bucket_name)
    async with db.connection_cursor() as (conn, cur):
        instance = await get_instance(instance_id, cur=cur)
        if instance is None:
            raise Exception('Instance not found')
        if instance['blocked']:
            raise Exception('Instance is blocked')
        await cur.execute('''
            INSERT INTO buckets (instance_id, name, public, blocked)
            VALUES (%s, %s, %s, False)
            ON CONFLICT DO NOTHING
            RETURNING name
        ''', (instance_id, bucket_name, public))
        assert await cur.fetchone(), 'Bucket already exists'
        async with AsyncExitStack() as exit_stack:
            await minio_api.create_bucket(bucket_name, exit_stack=exit_stack)
            if public:
                await minio_api.bucket_anonymous_set_download(bucket_name, exit_stack=exit_stack)
            await common.async_run_batches([
                minio_api.create_policy(policy, template.replace('__BUCKET_NAME__', bucket_name), exit_stack=exit_stack)
                for policy, template in [
                    (f'{bucket_name}_read', BUCKET_POLICY_READ_TEMPLATE),
                    (f'{bucket_name}_write', BUCKET_POLICY_WRITE_TEMPLATE),
                    (f'{bucket_name}_delete', BUCKET_POLICY_DELETE_TEMPLATE),
                ]
            ])
            instance_access_key = instance['access_key']
            await common.async_run_batches([
                minio_api.attach_policy_to_user(policy, instance_access_key, exit_stack=exit_stack)
                for policy in [
                    f'{bucket_name}_read',
                    f'{bucket_name}_write',
                    f'{bucket_name}_delete',
                ]
            ])
            await conn.commit()
            exit_stack.pop_all()
        return await get(instance_id, bucket_name, cur=cur)


async def update(instance_id, bucket_name, public=False, blocked=False):
    async with db.connection_cursor() as (conn, cur):
        instance = await get_instance(instance_id, cur=cur)
        if instance is None:
            raise Exception('Instance not found')
        bucket = await get(instance_id, bucket_name, cur=cur)
        if bucket is None:
            raise Exception('Bucket not found')
        await cur.execute('''
            UPDATE buckets
            SET public = %s, blocked = %s
            WHERE instance_id = %s AND name = %s
        ''', (public, blocked, instance_id, bucket_name))
        async with AsyncExitStack() as stack:
            if public and not bucket['public']:
                await minio_api.bucket_anonymous_set_download(bucket_name, exit_stack=stack)
            elif not public and bucket['public']:
                await minio_api.bucket_anonymous_set_none(bucket_name, exit_stack=stack)
            if blocked and not bucket['blocked']:
                await update_instance_access_key(bucket_name, instance['access_key'], None)
                credentials = [c async for c in credentials_list_iterator(instance_id, bucket_name, cur=cur)]
                await common.async_run_batches([
                    credentials_delete(instance_id, bucket_name, c['access_key'])
                    for c in credentials
                ])
            await conn.commit()
            stack.pop_all()
        return await get(instance_id, bucket_name, cur=cur)


async def update_instance_access_key(bucket_name, old_access_key, new_access_key):
    policies = [
        f'{bucket_name}_read',
        f'{bucket_name}_write',
        f'{bucket_name}_delete',
    ]
    async with AsyncExitStack() as stack:
        tasks = []
        tasks.extend([
            minio_api.detach_policy_from_user(policy, old_access_key, exit_stack=stack)
            for policy in policies
        ])
        if new_access_key:
            tasks.extend([
                minio_api.attach_policy_to_user(policy, new_access_key, exit_stack=stack)
                for policy in policies
            ])
        await common.async_run_batches(tasks)
        stack.pop_all()


async def delete(instance_id, bucket_name):
    async with db.connection_cursor() as (conn, cur):
        instance = await get_instance(instance_id, cur=cur)
        if instance is None:
            raise Exception('Instance not found')
        bucket = await get(instance_id, bucket_name, cur=cur)
        if bucket is None:
            raise Exception('Bucket not found')
        async with AsyncExitStack() as stack:
            await cur.execute('''
                DELETE FROM buckets
                WHERE instance_id = %s AND name = %s
            ''', (instance_id, bucket_name))
            await update_instance_access_key(bucket_name, instance['access_key'], None)
            await common.async_run_batches([
                minio_api.delete_policy(policy)
                for policy in [
                    f'{bucket_name}_read',
                    f'{bucket_name}_write',
                    f'{bucket_name}_delete',
                ]
            ])
            await minio_api.delete_bucket(bucket_name)
            await conn.commit()
            stack.pop_all()


async def list_iterator(instance_id, cur=None, with_size=False):
    total_size = 0
    async with db.connection_cursor(cur) as (conn, cur):
        await cur.execute('SELECT name FROM buckets WHERE instance_id = %s', (instance_id,))
        async for row in cur:
            if with_size:
                try:
                    size = await minio_api.get_bucket_size(row['name'])
                except:
                    traceback.print_exc()
                    size = None
                total_size += size
                yield {
                    'name': row['name'],
                    'size_bytes': size,
                }
            else:
                yield row['name']
    if with_size:
        yield {
            'name': '*',
            'size_bytes': total_size,
        }


async def get(instance_id, bucket_name, cur=None, with_size=False):
    async with db.connection_cursor(cur) as (conn, cur):
        await cur.execute('''
            SELECT public, blocked
            FROM buckets
            WHERE name = %s AND instance_id = %s
        ''', (bucket_name,instance_id))
        row = await cur.fetchone()
        if row is None:
            return None
        else:
            res = {
                'bucket_name': bucket_name,
                'instance_id': instance_id,
                'public': row['public'],
                'blocked': row['blocked']
            }
            if with_size:
                try:
                    size = await minio_api.get_bucket_size(bucket_name)
                except:
                    traceback.print_exc()
                    size = None
                res['size_bytes'] = size
            return res


async def list_buckets_prometheus_sd(targets):
    async with db.connection_cursor() as (conn, cur):
        await cur.execute('SELECT name FROM buckets')
        buckets = []
        async for row in cur:
            buckets.append({
                'targets': [t.strip() for t in targets.split(',') if t.strip()],
                'labels': {'bucket': row['name']}
            })
        return buckets


async def credentials_create(instance_id, bucket_name, read, write, delete):
    if not any([read, write, delete]):
        raise Exception('At least one permission must be specified')
    bucket = await get(instance_id, bucket_name)
    if bucket is None:
        raise Exception('Bucket not found')
    if bucket['blocked']:
        raise Exception('Bucket is blocked')
    async with db.connection_cursor() as (conn, cur):
        async with AsyncExitStack() as exit_stack:
            access_key = await access_keys.get_access_key(exit_stack)
            secret_key = common.generate_key(40)
            await cur.execute('''
                INSERT INTO bucket_credentials (instance_id, bucket_name, access_key, permission_read, permission_write, permission_delete)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (instance_id, bucket_name, access_key, read, write, delete))
            await minio_api.create_user(access_key, secret_key, exit_stack=exit_stack)
            policies = []
            if read:
                policies.append(f'{bucket_name}_read')
            if write:
                policies.append(f'{bucket_name}_write')
            if delete:
                policies.append(f'{bucket_name}_delete')
            await common.async_run_batches([
                minio_api.attach_policy_to_user(policy, access_key, exit_stack=exit_stack)
                for policy in policies
            ])
            await conn.commit()
            exit_stack.pop_all()
    return {
        'access_key': access_key,
        'secret_key': secret_key,
    }


async def credentials_delete(instance_id, bucket_name, access_key):
    async with db.connection_cursor() as (conn, cur):
        bucket = await get(instance_id, bucket_name, cur=cur)
        if bucket is None:
            raise Exception('Bucket not found')
        async with AsyncExitStack() as stack:
            await cur.execute('''
                SELECT 1 from bucket_credentials
                WHERE instance_id = %s AND bucket_name = %s AND access_key = %s
            ''', (instance_id, bucket_name, access_key))
            if await cur.fetchone() is None:
                raise Exception('Credentials not found')
            await cur.execute('''
                DELETE FROM bucket_credentials
                WHERE instance_id = %s AND bucket_name = %s AND access_key = %s
            ''', (instance_id, bucket_name, access_key))
            await common.async_run_batches([
                minio_api.detach_policy_from_user(policy, access_key, exit_stack=stack)
                for policy in [
                    f'{bucket_name}_read',
                    f'{bucket_name}_write',
                    f'{bucket_name}_delete',
                ]
            ])
            await minio_api.delete_user(access_key)
            await conn.commit()
            stack.pop_all()


async def credentials_list_iterator(instance_id, bucket_name, cur=None):
    async with db.connection_cursor(cur) as (conn, cur):
        await cur.execute('''
            SELECT access_key, permission_read, permission_write, permission_delete
            FROM bucket_credentials
            WHERE instance_id = %s AND bucket_name = %s
        ''', (instance_id, bucket_name))
        async for row in cur:
            yield {
                'access_key': row['access_key'],
                'permission_read': row['permission_read'],
                'permission_write': row['permission_write'],
                'permission_delete': row['permission_delete'],
            }
