import asyncclick as click
from fastapi import APIRouter

from . import api
from .. import common


router = APIRouter()


@click.group()
async def main():
    pass

@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@click.option('--public', is_flag=True)
@router.post('/buckets/create', tags=['buckets'])
async def create(instance_id: str, bucket_name: str, public: bool):
    return common.cli_print_json(await api.create(instance_id, bucket_name, public))


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@click.option('--public', is_flag=True)
@click.option('--blocked', is_flag=True)
@router.put('/buckets/update', tags=['buckets'])
async def update(instance_id: str, bucket_name: str, public: bool, blocked: bool):
    return common.cli_print_json(await api.update(instance_id, bucket_name, public, blocked))


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@router.delete('/buckets/delete', tags=['buckets'])
async def delete(instance_id: str, bucket_name: str):
    return common.cli_print_json(await api.delete(instance_id, bucket_name))


@main.command(name='list')
@click.argument('instance_id')
@router.get('/buckets/list', tags=['buckets'])
async def list_buckets(instance_id: str):
    buckets = [bucket async for bucket in api.list_iterator(instance_id)]
    if common.is_cli():
        common.cli_print_json(buckets)
        return click.echo(f'Total buckets: {len(buckets)}', err=True)
    else:
        return buckets


@main.command()
@click.argument('targets')
@router.get('/buckets/list_prometheus_sd', include_in_schema=False)
async def list_buckets_prometheus_sd(targets: str):
    return common.cli_print_json(await api.list_buckets_prometheus_sd(targets))


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@router.get('/buckets/get', tags=['buckets'])
async def get(instance_id: str, bucket_name: str):
    bucket = await api.get(instance_id, bucket_name)
    if common.is_cli():
        return common.cli_print_json(bucket)
    else:
        if bucket is None:
            return {'error': 'Bucket not found'}
        return bucket


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@click.option('--read', is_flag=True)
@click.option('--write', is_flag=True)
@click.option('--delete', is_flag=True)
@router.post('/buckets/credentials_create', tags=['buckets'])
async def credentials_create(instance_id: str, bucket_name: str, read: bool, write: bool, delete: bool):
    return common.cli_print_json(await api.credentials_create(instance_id, bucket_name, read, write, delete))


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@click.argument('access_key')
@router.delete('/buckets/credentials_delete', tags=['buckets'])
async def credentials_delete(instance_id: str, bucket_name: str, access_key: str):
    return common.cli_print_json(await api.credentials_delete(instance_id, bucket_name, access_key))


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@router.get('/buckets/credentials_list', tags=['buckets'])
async def credentials_list(instance_id: str, bucket_name: str):
    creds = [cred async for cred in api.credentials_list_iterator(instance_id, bucket_name)]
    if common.is_cli():
        common.cli_print_json(creds)
        return click.echo(f'Total credentials: {len(creds)}', err=True)
    else:
        return creds
