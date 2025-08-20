import sys

import asyncclick as click

from .. import common


@click.group()
async def main():
    pass


@main.command()
@click.argument('instance_id')
async def create(**kwargs):
    from .api import create
    common.json_print(await create(**kwargs))


@main.command()
@click.argument('instance_id')
@click.option('--blocked', type=click.Choice(('true', 'false'), case_sensitive=False))
async def update(**kwargs):
    if kwargs['blocked'] is not None:
        kwargs['blocked'] = kwargs['blocked'].lower() == 'true'
    from .api import update
    common.json_print(await update(**kwargs))


@main.command()
@click.argument('instance_id')
async def delete(**kwargs):
    from .api import delete
    common.json_print(await delete(**kwargs))


@main.command()
@click.argument('instance_id')
async def get(**kwargs):
    from .api import get
    common.json_print(await get(**kwargs))


@main.command(name='list')
async def list_():
    from .api import list_iterator
    num_instances = 0
    async for instance_id in list_iterator():
        print(instance_id)
        num_instances += 1
    print(f'Total instances: {num_instances}', file=sys.stderr)
