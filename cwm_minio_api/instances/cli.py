import sys

import asyncclick as click

from .. import common


@click.group()
async def main():
    pass


@main.command()
@click.argument('instance_id')
@click.option('--max-size-gb', type=int, default=10)
async def create(**kwargs):
    from .api import create
    common.json_print(await create(**kwargs))


@main.command()
@click.argument('instance_id')
@click.option('--max-size-gb', type=int)
async def update(**kwargs):
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
