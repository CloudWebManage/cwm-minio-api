import asyncclick as click
from fastapi import APIRouter
from pydantic import BaseModel

from . import api
from .. import common


router = APIRouter()


@click.group()
async def main():
    pass


class CreateRequest(BaseModel):
    instance_id: str
    bucket_name: str
    public: bool = False


@router.post('/buckets/create', tags=['buckets'])
async def create(request: CreateRequest):
    return common.cli_print_json(await api.create(request.instance_id, request.bucket_name, request.public))


class UpdateRequest(BaseModel):
    instance_id: str
    bucket_name: str
    public: bool
    blocked: bool


@router.put('/buckets/update', tags=['buckets'])
async def update(request: UpdateRequest):
    return common.cli_print_json(await api.update(request.instance_id, request.bucket_name, request.public, request.blocked))


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@router.delete('/buckets/delete', tags=['buckets'])
async def delete(instance_id: str, bucket_name: str):
    return common.cli_print_json(await api.delete(instance_id, bucket_name))


@main.command(name='list')
@click.argument('instance_id')
@click.option('--with_size', is_flag=True)
@router.get('/buckets/list', tags=['buckets'])
async def list_buckets(instance_id: str, with_size: bool = False):
    buckets = [bucket async for bucket in api.list_iterator(instance_id, with_size=with_size)]
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
@click.option('--with-size', is_flag=True)
@router.get('/buckets/get', tags=['buckets'])
async def get(instance_id: str, bucket_name: str, with_size: bool = False):
    bucket = await api.get(instance_id, bucket_name, with_size=with_size)
    if common.is_cli():
        return common.cli_print_json(bucket)
    else:
        if bucket is None:
            return {'error': 'Bucket not found'}
        return bucket


class CredentialsCreateRequest(BaseModel):
    instance_id: str
    bucket_name: str
    read: bool
    write: bool
    delete: bool


@router.post('/buckets/credentials', tags=['buckets'])
async def credentials_create(request: CredentialsCreateRequest):
    return common.cli_print_json(await api.credentials_create(request.instance_id, request.bucket_name, request.read, request.write, request.delete))


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@click.argument('access_key')
@router.delete('/buckets/credentials', tags=['buckets'])
async def credentials_delete(instance_id: str, bucket_name: str, access_key: str):
    return common.cli_print_json(await api.credentials_delete(instance_id, bucket_name, access_key))


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@router.get('/buckets/credentials', tags=['buckets'])
async def credentials_list(instance_id: str, bucket_name: str):
    creds = [cred async for cred in api.credentials_list_iterator(instance_id, bucket_name)]
    if common.is_cli():
        common.cli_print_json(creds)
        return click.echo(f'Total credentials: {len(creds)}', err=True)
    else:
        return creds
