import logging
import importlib

from fastapi import APIRouter


router = APIRouter()


@router.get("/", include_in_schema=False)
async def root():
    logging.debug('Root endpoint called')
    return {"ok": True}



for submodule in [
    'instances',
    'buckets',
]:
    router.include_router(getattr(importlib.import_module(f'.{submodule}.router', __package__), 'router'))
