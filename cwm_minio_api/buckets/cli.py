import sys
import asyncclick as click

from .. import common


@click.group()
async def main():
    pass


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@click.option('--public', is_flag=True, default=False)
async def create(**kwargs):
    from .api import create
    common.json_print(await create(**kwargs))


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
@click.option('--public', is_flag=True, default=False)
async def update(**kwargs):
    from .api import update
    common.json_print(await update(**kwargs))


@main.command()
@click.argument('instance_id')
@click.argument('bucket_name')
async def delete(**kwargs):
    from .api import delete
    common.json_print(await delete(**kwargs))


@main.command(name='list')
@click.argument('instance_id')
async def list_(**kwargs):
    from .api import list_iterator
    num_buckets = 0
    async for bucket_name in list_iterator(**kwargs):
        print(bucket_name)
        num_buckets += 1
    print(f'Total buckets: {num_buckets}', file=sys.stderr)


@main.command()
@click.argument('bucket_name')
async def get(**kwargs):
    from .api import get
    common.json_print(await get(**kwargs))


from .reports import cli as reports_cli
main.add_command(reports_cli.main, name='reports')
