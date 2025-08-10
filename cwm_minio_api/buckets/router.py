from click import group
from fastapi import APIRouter

from . import api


router = APIRouter()


@router.get('/buckets/create', tags=['buckets'])
async def create(instance_id: str, bucket_name: str, public: bool = False):
    return await api.create(instance_id, bucket_name, public)


@router.get('/buckets/update', tags=['buckets'])
async def update(instance_id: str, bucket_name: str, public: bool = False):
    return await api.update(instance_id, bucket_name, public)


@router.get('/buckets/delete', tags=['buckets'])
async def delete(instance_id: str, bucket_name: str):
    return await api.delete(instance_id, bucket_name)


@router.get('/buckets/list', tags=['buckets'])
async def list_buckets(instance_id: str):
    return [bucket async for bucket in api.list_iterator(instance_id)]


@router.get('/buckets/get', tags=['buckets'])
async def get(instance_id: str, bucket_name: str):
    bucket = await api.get(instance_id, bucket_name)
    if bucket is None:
        return {'error': 'Bucket not found'}
    return bucket

