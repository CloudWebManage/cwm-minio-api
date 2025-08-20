import asyncio

import orjson


def json_print(data):
    print(orjson.dumps(data, option=(orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)).decode())


async def async_subprocess_check_call(*args, **kwargs):
    assert (await (await asyncio.create_subprocess_exec(*args, **kwargs)).wait()) == 0


async def async_subprocess_check_output(*args, **kwargs):
    proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, **kwargs)
    stdout, _ = await proc.communicate()
    assert proc.returncode == 0
    return stdout.decode().strip()


async def async_subprocess_status_output(*args, **kwargs):
    proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, **kwargs)
    stdout, _ = await proc.communicate()
    return proc.returncode, stdout.decode().strip()
