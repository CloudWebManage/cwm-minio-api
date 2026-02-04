import asyncclick as click


@click.group()
async def main():
    pass


@main.command()
async def cleanup():
    from . import cleanup
    await cleanup.main()
