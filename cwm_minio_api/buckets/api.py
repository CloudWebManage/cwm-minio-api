import re
import string
import secrets
from textwrap import dedent
from contextlib import AsyncExitStack

from .. import db
from ..instances.api import get as get_instance
from ..minio import api as minio_api


# https://docs.min.io/community/minio-object-store/administration/identity-access-management/policy-based-access-control.html
BUCKET_POLICY_TEMPLATE = dedent('''
{
   "Version" : "2012-10-17",
   "Statement" : [
      {
         "Effect" : "Allow",
         "Action" : [
            "s3:GetBucketLocation",
            "s3:ListAllMyBuckets",
            "s3:DeleteObject",
            "s3:GetObject",
            "s3:GetObjectAttributes",
            "s3:GetObjectVersionAttributes",
            "s3:RestoreObject",
            "s3:ListBucket",
            "s3:PutObject",
            "s3:PutObjectTagging",
            "s3:GetObjectTagging",
            "s3:DeleteObjectTagging",
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


def check_bucket_name(bucket_name):
    # based on https://github.com/minio/minio-go/blob/54af66a15eeca47d177eac8162376006485d7ae7/pkg/s3utils/utils.go#L348
    if not bucket_name or not bucket_name.strip():
        raise ValueError('Bucket name cannot be empty')
    if len(bucket_name) < 3:
        raise ValueError('Bucket name cannot be shorter than 3 characters')
    if len(bucket_name) > 63:
        raise ValueError('Bucket name cannot be longer than 63 characters')
    ip_address_regex = r'^(\d+\.){3}\d+$'
    if re.match(ip_address_regex, bucket_name):
        raise ValueError('Bucket name cannot be an IP address')
    if '..' in bucket_name or '.-' in bucket_name or '-.' in bucket_name:
        raise ValueError('Bucket name contains invalid characters')
    valid_bucket_name_strict = r'^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$'
    if not re.match(valid_bucket_name_strict, bucket_name):
        raise ValueError('Bucket name contains invalid characters')
    valid_bucket_name = r'^[A-Za-z0-9][A-Za-z0-9\.\-\_\:]{1,61}[A-Za-z0-9]$'
    if not re.match(valid_bucket_name, bucket_name):
        raise ValueError('Bucket name contains invalid characters')


def generate_key(length):
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(length))


async def create(instance_id, bucket_name, public):
    check_bucket_name(bucket_name)
    async with db.connection_cursor() as (conn, cur):
        bucket = await get(bucket_name, cur=cur)
        if bucket is not None:
            if bucket['instance_id'] == instance_id:
                raise Exception('Bucket already exists in this instance')
            else:
                raise Exception('Bucket name already taken in this tenant')
        instance = await get_instance(instance_id, cur=cur)
        if instance is None:
            raise Exception('Instance not found')
        if instance['status'] != 'active':
            raise Exception('Instance is not active')
        access_key = bucket_name + ':' + generate_key(10)
        await cur.execute('''
            INSERT INTO buckets (instance_id, name, public, access_key)
            VALUES (%s, %s, %s, %s)
        ''', (instance_id, bucket_name, public, access_key))
        async with AsyncExitStack() as stack:
            await minio_api.create_bucket(bucket_name, exit_stack=stack)
            secret_key = generate_key(40)
            await minio_api.create_user(access_key, secret_key, exit_stack=stack)
            await minio_api.create_policy(bucket_name, BUCKET_POLICY_TEMPLATE.replace('__BUCKET_NAME__', bucket_name), exit_stack=stack)
            await minio_api.attach_policy_to_user(bucket_name, access_key, exit_stack=stack)
            if public:
                await minio_api.bucket_anonymous_set_download(bucket_name, exit_stack=stack)
            await conn.commit()
            stack.pop_all()
        return {
            **await get(bucket_name, cur=cur),
            'secret_key': secret_key
        }


async def update(instance_id, bucket_name, public):
    async with db.connection_cursor() as (conn, cur):
        bucket = await get(bucket_name, cur=cur)
        if bucket is None:
            raise Exception('Bucket not found')
        if bucket['instance_id'] != instance_id:
            raise Exception('Bucket does not belong to the specified instance')
        await cur.execute('''
            UPDATE buckets
            SET public = %s
            WHERE instance_id = %s AND name = %s
        ''', (public, instance_id, bucket_name))
        async with AsyncExitStack() as stack:
            if public:
                await minio_api.bucket_anonymous_set_download(bucket_name, exit_stack=stack)
            else:
                await minio_api.bucket_anonymous_set_none(bucket_name)
            await conn.commit()
        return await get(bucket_name, cur=cur)


async def delete(instance_id, bucket_name):
    async with db.connection_cursor() as (conn, cur):
        bucket = await get(bucket_name, cur=cur)
        if bucket is None:
            raise Exception('Bucket not found')
        if bucket['instance_id'] != instance_id:
            raise Exception('Bucket does not belong to the specified instance')
        await cur.execute('''
            DELETE FROM buckets
            WHERE instance_id = %s AND name = %s
        ''', (instance_id, bucket_name))
        await minio_api.detach_policy_from_user(bucket_name, bucket['access_key'])
        await minio_api.delete_policy(bucket_name)
        await minio_api.delete_user(bucket['access_key'])
        await minio_api.delete_bucket(bucket_name)
        await conn.commit()


async def list_iterator(instance_id):
    async with db.connection_cursor() as (conn, cur):
        if await get_instance(instance_id, cur=cur) is None:
            raise Exception('Instance not found')
        await cur.execute('SELECT name FROM buckets WHERE instance_id = %s', (instance_id,))
        async for row in cur:
            yield row['name']


async def get(bucket_name, cur=None):
    async with db.connection_cursor(cur) as (conn, cur):
        await cur.execute('''
            SELECT instance_id, public, access_key
            FROM buckets
            WHERE name = %s
        ''', (bucket_name,))
        row = await cur.fetchone()
        if row is None:
            return None
        else:
            return {
                'status': 'active',
                'bucket_name': bucket_name,
                'instance_id': row['instance_id'],
                'public': row['public'],
                'access_key': row['access_key'],
            }
