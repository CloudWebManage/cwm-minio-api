import asyncclick as click


@click.group()
async def main():
    pass


@main.command()
async def cleanup():
    from . import cleanup
    await cleanup.main()


@main.command()
@click.argument("filename")
async def export_shared_state(filename):
    from .shared_state import SharedState
    SharedState.get_singleton().export(filename)
