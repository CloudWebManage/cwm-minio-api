from fastapi import APIRouter

from . import api


router = APIRouter()


@router.get('/instances/create', tags=['instances'])
async def create(instance_id: str, max_size_gb: int):
    return await api.create(instance_id, max_size_gb)


@router.get('/instances/update', tags=['instances'])
async def update(instance_id: str, max_size_gb: int):
    return await api.update(instance_id, max_size_gb)


@router.get('/instances/delete', tags=['instances'])
async def delete(instance_id: str):
    return await api.delete(instance_id)


@router.get('/instances/get', tags=['instances'])
async def get(instance_id: str):
    instance = await api.get(instance_id)
    if instance is None:
        raise Exception('Instance not found')
    return instance


@router.get('/instances/list', tags=['instances'])
async def list_instances():
    return [instance_id async for instance_id in api.list_iterator()]
