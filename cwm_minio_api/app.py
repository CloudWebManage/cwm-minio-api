import logging
import traceback

from fastapi import FastAPI, logger, Request
from fastapi.responses import ORJSONResponse

from .version import VERSION
from .router import router
from . import config, common


async def global_exception_handler(request: Request, exc: Exception):
    status_code = 500
    if isinstance(exc, common.ServerOverloadedException):
        status_code = 503
    return ORJSONResponse(
        status_code=status_code,
        content={
            "exception": str(exc),
            "traceback": traceback.format_exception(exc),
        }
    )


def app():
    app_ = FastAPI(
        version=VERSION,
        title='CWM MinIO API',
    )
    if config.CWM_ENV_TYPE == 'docker':
        logging.basicConfig(level=getattr(logging, config.CWM_LOG_LEVEL), handlers=logging.getLogger("gunicorn.error").handlers)
    else:
        logging.basicConfig(level=getattr(logging, config.CWM_LOG_LEVEL), handlers=logger.logger.handlers)
    app_.add_exception_handler(Exception, global_exception_handler)
    app_.include_router(router)
    logging.info('App initialized')
    return app_
