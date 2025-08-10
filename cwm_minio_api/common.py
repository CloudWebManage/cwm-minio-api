import orjson


def json_print(data):
    print(orjson.dumps(data, option=(orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)).decode())
