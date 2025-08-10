import asyncclick as click

from ... import common


@click.group()
async def main():
    pass


@main.command()
@click.argument('instance_id')
@click.argument('report_name')
@click.argument('report_type')
@click.argument('start_date')
@click.argument('end_date')
async def get(instance_id, report_name, report_type, start_date, end_date):
    from .api import get
    common.json_print(await get(instance_id, report_name, report_type, start_date, end_date))
