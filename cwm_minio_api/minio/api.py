import asyncio
import logging
import tempfile

import orjson

from .. import config, common


async def mc_check_call(*args, return_output=False):
    logging.debug(f'mc_check_call({" ".join(args)})')
    proc = await asyncio.create_subprocess_exec(config.MINIO_MC_BINARY, *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    stdout, _ = await proc.communicate()
    stdout = stdout.decode().strip()
    logging.debug(f'mc_check_call({" ".join(args)}): {stdout}')
    assert proc.returncode == 0, stdout
    return stdout if return_output else None


async def mc_check_output(*args):
    return await mc_check_call(*args, return_output=True)


async def create_bucket(name, exit_stack=None):
    await mc_check_call('mb', f'{config.MINIO_MC_PROFILE}/{name}')
    if exit_stack:
        exit_stack.push_async_callback(delete_bucket, name)


async def delete_bucket(name):
    await mc_check_call('rb', f'{config.MINIO_MC_PROFILE}/{name}', '--force')


async def bucket_exists(name):
    try:
        await mc_check_call('ls', f'{config.MINIO_MC_PROFILE}/{name}')
        return True
    except Exception:
        return False


async def create_policy(name, policy_json, exit_stack=None):
    with tempfile.NamedTemporaryFile() as policy_file:
        policy_file.write(policy_json.encode())
        policy_file.flush()
        policy_filename = policy_file.name
        await mc_check_call('admin', 'policy', 'create', config.MINIO_MC_PROFILE, name, policy_filename)
    if exit_stack:
        exit_stack.push_async_callback(delete_policy, name)


async def delete_policy(name):
    await mc_check_call('admin', 'policy', 'rm', config.MINIO_MC_PROFILE, name)


async def create_user(user, password, exit_stack=None):
    await mc_check_call('admin', 'user', 'add', config.MINIO_MC_PROFILE, user, password)
    if exit_stack:
        exit_stack.push_async_callback(delete_user, user)


async def delete_user(user):
    await mc_check_call('admin', 'user', 'rm', config.MINIO_MC_PROFILE, user)


async def attach_policy_to_user(policy_name, user_name, exit_stack=None):
    await mc_check_call('admin', 'policy', 'attach', config.MINIO_MC_PROFILE, policy_name, '--user', user_name)
    if exit_stack:
        exit_stack.push_async_callback(detach_policy_from_user, policy_name, user_name)


async def detach_policy_from_user(policy_name, user_name, exit_stack=None):
    await mc_check_call('admin', 'policy', 'detach', config.MINIO_MC_PROFILE, policy_name, '--user', user_name)
    if exit_stack:
        exit_stack.push_async_callback(attach_policy_to_user, policy_name, user_name)


async def bucket_anonymous_set_download(bucket_name, exit_stack=None):
    await mc_check_call('anonymous', 'set', 'download', f'{config.MINIO_MC_PROFILE}/{bucket_name}')
    if exit_stack:
        exit_stack.push_async_callback(bucket_anonymous_set_none, bucket_name)


async def bucket_anonymous_set_none(bucket_name, exit_stack=None):
    await mc_check_call('anonymous', 'set', 'none', f'{config.MINIO_MC_PROFILE}/{bucket_name}')
    if exit_stack:
        exit_stack.push_async_callback(bucket_anonymous_set_download, bucket_name)


async def get_bucket_size(bucket_name):
    stat = orjson.loads(await mc_check_output('stat', f'{config.MINIO_MC_PROFILE}/{bucket_name}', '--json'))
    return stat.get('Usage', {}).get('size')
