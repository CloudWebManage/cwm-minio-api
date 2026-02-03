from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .prometheus import get_prometheus_registry


router = APIRouter()


@router.get("/metrics", include_in_schema=False)
async def metrics():
    registry = get_prometheus_registry()
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
