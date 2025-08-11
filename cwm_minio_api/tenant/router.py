from fastapi import APIRouter

from .. import config


router = APIRouter()


@router.get('/tenant/info', tags=['tenant'])
async def info():
    return config.TENANT_INFO
