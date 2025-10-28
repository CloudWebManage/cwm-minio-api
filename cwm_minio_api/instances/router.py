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


@router.post('/instances/create', tags=['instances'])
async def create(request: CreateRequest):
    return common.cli_print_json(await api.create(request.instance_id))


class UpdateRequest(BaseModel):
    instance_id: str
    blocked: bool
    reset_access_key: bool


@router.put('/instances/update', tags=['instances'])
async def update(request: UpdateRequest):
    return common.cli_print_json(await api.update(request.instance_id, request.blocked, request.reset_access_key))


@main.command()
@click.argument('instance_id')
@router.delete('/instances/delete', tags=['instances'])
async def delete(instance_id: str):
    return common.cli_print_json(await api.delete(instance_id))


@main.command()
@click.argument('instance_id')
@router.get('/instances/get', tags=['instances'])
async def get(instance_id: str):
    instance = await api.get(instance_id)
    if instance is None:
        raise Exception('Instance not found')
    return common.cli_print_json(instance)


@main.command()
@router.get('/instances/list', tags=['instances'])
async def list_instances():
    instances = [instance_id async for instance_id in api.list_iterator()]
    if common.is_cli():
        common.cli_print_json(instances)
        return click.echo(f'Total instances: {len(instances)}', err=True)
    else:
        return instances
