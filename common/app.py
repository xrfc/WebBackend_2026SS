import logging
import uuid
from fastapi import FastAPI, Request, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError
from common.config import settings
from common.response import ApiResponse

logger = logging.getLogger('webbackend')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')


def create_app(title, lifespan=None):
    app = FastAPI(title=title, version='2.0.0', lifespan=lifespan)

    @app.middleware('http')
    async def bounded_request(request: Request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        try:
            length = int(request.headers.get('content-length', '0'))
            if length < 0 or length > settings.MAX_BODY_BYTES:
                return ApiResponse.fail(413, '请求体过大')
        except ValueError:
            return ApiResponse.fail(400, 'Content-Length 无效')
        # Read incrementally, also bounding chunked bodies. The cached body is forwarded by Starlette.
        chunks, total = [], 0
        async for chunk in request.stream():
            total += len(chunk)
            if total > settings.MAX_BODY_BYTES:
                return ApiResponse.fail(413, '请求体过大')
            chunks.append(chunk)
        request._body = b''.join(chunks)
        response = await call_next(request)
        response.headers['X-Request-ID'] = request_id
        return response

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        return ApiResponse.fail(exc.status_code, str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        errors = [{'field': '.'.join(map(str, error['loc'])), 'message': error['msg']}
                  for error in exc.errors()]
        return JSONResponse(status_code=422, content={'code': 422, 'message': '参数校验失败', 'data': errors})

    @app.exception_handler(RedisError)
    @app.exception_handler(SQLAlchemyError)
    async def dependency_error(request, exc):
        logger.error('dependency failure request=%s type=%s', request.state.request_id, type(exc).__name__)
        return ApiResponse.fail(503, '依赖服务暂时不可用，请稍后重试')

    @app.exception_handler(Exception)
    async def unexpected_error(request, exc):
        logger.error('unhandled failure request=%s type=%s', request.state.request_id, type(exc).__name__)
        return ApiResponse.fail(500, '服务内部异常，请提供 X-Request-ID 排查')

    @app.get('/health')
    async def health():
        return {'status': 'ok'}

    return app
