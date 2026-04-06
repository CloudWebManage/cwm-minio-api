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


@router.post('/credentials', tags=['credentials'])
async def create(request: CreateRequest):
    return common.cli_print_json(await api.create(request.instance_id))


@main.command(name='create')
@click.argument('instance_id')
async def create_cli(instance_id: str):
    return common.cli_print_json(await api.create(instance_id))


@main.command()
@click.argument('access_key')
@router.delete('/credentials', tags=['credentials'])
async def delete(access_key: str):
    return common.cli_print_json(await api.delete(access_key))


@main.command(name='list')
@click.argument('instance_id')
@router.get('/credentials', tags=['credentials'])
async def list_credentials(instance_id: str):
    credentials = [credential async for credential in api.list_iterator(instance_id)]
    if common.is_cli():
        common.cli_print_json(credentials)
        return click.echo(f'Total credentials: {len(credentials)}', err=True)
    return credentials
