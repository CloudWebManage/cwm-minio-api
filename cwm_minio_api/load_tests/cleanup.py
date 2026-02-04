import json

from ..minio import api as minio_api
from ..config import MINIO_MC_PROFILE


async def main():
    for line in (await minio_api.mc_check_output('ls', MINIO_MC_PROFILE, '--json')).splitlines():
        print(json.loads(line))
    raise NotImplementedError("Cleanup script is not implemented yet.")
