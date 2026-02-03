from ..minio import api as minio_api
from ..config import MINIO_MC_PROFILE


def main():
    minio_api.mc_check_output(['ls', MINIO_MC_PROFILE])
    raise NotImplementedError("Cleanup script is not implemented yet.")
