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
@router.post('/instances/create', tags=['instances'])
async def create(instance_id: str):
    return common.cli_print_json(await api.create(instance_id))


@main.command()
@click.argument('instance_id')
@click.option('--blocked', is_flag=True)
@click.option('--reset-access-key', is_flag=True)
@router.put('/instances/update', tags=['instances'])
async def update(instance_id: str, blocked: bool, reset_access_key: bool):
    return common.cli_print_json(await api.update(instance_id, blocked, reset_access_key))


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
