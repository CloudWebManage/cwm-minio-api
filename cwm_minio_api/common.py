import re
import string
import secrets
import asyncio


import orjson
import asyncclick as click


def is_cli():
    return click.get_current_context(silent=True) is not None


def cli_print_json(data):
    if is_cli():
        click.echo(orjson.dumps(data, option=(orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE)).decode())
    return data


async def async_subprocess_check_call(*args, **kwargs):
    assert (await (await asyncio.create_subprocess_exec(*args, **kwargs)).wait()) == 0, f'Command {" ".join(args)} failed, check logs for details'


async def async_subprocess_check_output(*args, **kwargs):
    proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, **kwargs)
    stdout, _ = await proc.communicate()
    assert proc.returncode == 0
    return stdout.decode().strip()


async def async_subprocess_status_output(*args, **kwargs):
    proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, **kwargs)
    stdout, _ = await proc.communicate()
    return proc.returncode, stdout.decode().strip()


def generate_key(length):
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(length))


def check_bucket_name(bucket_name):
    # based on https://github.com/minio/minio-go/blob/54af66a15eeca47d177eac8162376006485d7ae7/pkg/s3utils/utils.go#L348
    if not bucket_name or not bucket_name.strip():
        raise ValueError('Bucket name cannot be empty')
    if len(bucket_name) < 3:
        raise ValueError('Bucket name cannot be shorter than 3 characters')
    if len(bucket_name) > 63:
        raise ValueError('Bucket name cannot be longer than 63 characters')
    ip_address_regex = r'^(\d+\.){3}\d+$'
    if re.match(ip_address_regex, bucket_name):
        raise ValueError('Bucket name cannot be an IP address')
    if '..' in bucket_name or '.-' in bucket_name or '-.' in bucket_name:
        raise ValueError("Bucket name contains invalid characters")
    valid_bucket_name_strict = r'^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$'
    if not re.match(valid_bucket_name_strict, bucket_name):
        raise ValueError('Bucket name contains invalid characters')
    valid_bucket_name = r'^[A-Za-z0-9][A-Za-z0-9\.\-\_\:]{1,61}[A-Za-z0-9]$'
    if not re.match(valid_bucket_name, bucket_name):
        raise ValueError('Bucket name contains invalid characters')


def check_instance_id(instance_id):
    if not instance_id or not instance_id.strip():
        raise ValueError('Instance ID cannot be empty')
    if len(instance_id) < 3:
        raise ValueError('Instance ID cannot be shorter than 3 characters')
    if len(instance_id) > 63:
        raise ValueError('Instance ID cannot be longer than 63 characters')
    if not re.match(r'^[a-zA-Z0-9\.\-\_\:]+$', instance_id):
        raise ValueError('Instance ID contains invalid characters')


async def async_run_batches(tasks, batch_size=10):
    for i in range(0, len(tasks), batch_size):
        async with asyncio.TaskGroup() as tg:
            for task in tasks[i:i + batch_size]:
                tg.create_task(task)


async def wait_for(condition_coro, timeout, check_interval=0.5):
    start_time = asyncio.get_event_loop().time()
    while True:
        if await condition_coro():
            return
        if asyncio.get_event_loop().time() - start_time > timeout:
            raise TimeoutError('Timeout waiting for condition')
        await asyncio.sleep(check_interval)
