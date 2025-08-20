import asyncio

import orjson


def json_print(data):
    print(orjson.dumps(data, option=(orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)).decode())


async def async_subprocess_check_call(*args, **kwargs):
    assert (await (await asyncio.create_subprocess_exec(*args, **kwargs)).wait()) == 0
