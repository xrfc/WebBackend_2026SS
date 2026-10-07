import asyncio
import time
import copy
from contextlib import asynccontextmanager
from urllib.parse import urlsplit
import httpx
import websockets
from fastapi import Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from common.app import create_app
from common.config import settings
from common.redis_client import redis_client
from common.limits import rate_limit

ROUTE_MAP = {
    '/register': settings.USER_SERVICE_URL, '/login': settings.USER_SERVICE_URL,
    '/logout': settings.USER_SERVICE_URL, '/me': settings.USER_SERVICE_URL,
    '/products': settings.PRODUCT_SERVICE_URL, '/orders': settings.ORDER_SERVICE_URL,
    '/seckill': settings.SECKILL_SERVICE_URL, '/ai': settings.AI_SERVICE_URL,
}
HOP_HEADERS = {'host', 'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
               'te', 'trailer', 'transfer-encoding', 'upgrade', 'content-length'}


def get_target_service(path):
    for prefix, url in ROUTE_MAP.items():
        if path == prefix or path.startswith(prefix + '/'):
            return url
    raise HTTPException(404, '接口不存在')


@asynccontextmanager
async def lifespan(app):
    await redis_client.ping()
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(25, connect=3),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=50))
    yield
    await app.state.http.aclose()
    await redis_client.aclose()


app = create_app('API Gateway', lifespan=lifespan)
app.add_middleware(CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.CORS_ORIGINS.split(',') if origin.strip()],
    allow_credentials=True, allow_methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
    allow_headers=['Authorization', 'Content-Type'])


@app.get('/ready')
async def ready():
    await redis_client.ping()
    results = await asyncio.gather(*[app.state.http.get(url + '/ready') for url in sorted(set(ROUTE_MAP.values()))],
        return_exceptions=True)
    if any(isinstance(result, Exception) or result.status_code != 200 for result in results):
        raise HTTPException(503, '部分后端服务尚未就绪')
    return {'status': 'ready'}


@app.websocket('/ws/{user_id}')
async def websocket_proxy(ws: WebSocket, user_id: int):
    target = urlsplit(settings.ORDER_SERVICE_URL)
    url = f'{"wss" if target.scheme == "https" else "ws"}://{target.netloc}{target.path}/ws/{user_id}'
    headers = {key: ws.headers[key] for key in ('authorization', 'origin') if key in ws.headers}
    await ws.accept()
    tasks = []
    try:
        async with websockets.connect(url, extra_headers=headers, open_timeout=3,
            max_size=settings.MAX_BODY_BYTES, max_queue=16) as upstream:
            async def to_upstream():
                while True:
                    item = await ws.receive()
                    if item['type'] == 'websocket.disconnect':
                        return
                    await upstream.send(item.get('text') if item.get('text') is not None else item['bytes'])

            async def to_client():
                async for item in upstream:
                    if isinstance(item, bytes):
                        await ws.send_bytes(item)
                    else:
                        await ws.send_text(item)
                await ws.close(code=upstream.close_code or 1000)

            tasks = [asyncio.create_task(to_upstream()), asyncio.create_task(to_client())]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
    except (WebSocketDisconnect, websockets.ConnectionClosed):
        try:
            await ws.close(code=1008)
        except Exception:
            pass
    except Exception:
        try:
            await ws.close(code=1013)
        except Exception:
            pass
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


# Replace the gateway-only schema with the actual five backend API schemas.
app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/openapi.json']


@app.get('/openapi.json', include_in_schema=False)
async def aggregate_openapi():
    cached = getattr(app.state, 'schema_cache', None)
    if cached and time.monotonic() - cached[0] < 60:
        return cached[1]
    services = {'user': settings.USER_SERVICE_URL, 'product': settings.PRODUCT_SERVICE_URL,
        'order': settings.ORDER_SERVICE_URL, 'seckill': settings.SECKILL_SERVICE_URL, 'ai': settings.AI_SERVICE_URL}
    documents = await asyncio.gather(*[app.state.http.get(url + '/openapi.json') for url in services.values()],
        return_exceptions=True)
    result = {'openapi': '3.1.0', 'info': {'title': 'WebBackend API', 'version': '2.0.0'},
        'paths': {}, 'components': {'schemas': {}, 'securitySchemes': {}}}
    def rename_refs(value, prefix):
        if isinstance(value, dict):
            for key, item in value.items():
                if key == '$ref' and isinstance(item, str) and item.startswith('#/components/schemas/'):
                    value[key] = '#/components/schemas/' + prefix + '_' + item.rsplit('/', 1)[1]
                else:
                    rename_refs(item, prefix)
        elif isinstance(value, list):
            for item in value:
                rename_refs(item, prefix)
    for name, response in zip(services, documents):
        if isinstance(response, Exception) or response.status_code != 200:
            raise HTTPException(503, 'API 文档暂时不可用')
        document = copy.deepcopy(response.json())
        rename_refs(document, name)
        for path, operations in document['paths'].items():
            if path in ('/health', '/ready'):
                continue
            for operation in operations.values():
                if isinstance(operation, dict):
                    operation['tags'] = [name]
                    operation['operationId'] = name + '_' + operation.get('operationId', 'operation')
            result['paths'][path] = operations
        for key, schema in document.get('components', {}).get('schemas', {}).items():
            result['components']['schemas'][name + '_' + key] = schema
        result['components']['securitySchemes'].update(document.get('components', {}).get('securitySchemes', {}))
    app.state.schema_cache = (time.monotonic(), result)
    return result


@app.api_route('/{path:path}', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
async def gateway_proxy(request: Request, path: str):
    request_path = '/' + path
    target = get_target_service(request_path)
    if request_path == '/login' or request_path.startswith('/register'):
        # Do not trust externally supplied X-Forwarded-For.
        await rate_limit('gateway-auth:' + request.client.host, 120, 60)
    headers = {key: value for key, value in request.headers.items() if key.lower() not in HOP_HEADERS}
    headers['X-Request-ID'] = request.state.request_id
    try:
        response = await app.state.http.request(request.method, target + request_path,
            params=request.query_params.multi_items(), headers=headers, content=await request.body())
        # HTTPX decodes response bodies; don't forward stale encoding/length headers.
        response_headers = {key: value for key, value in response.headers.items()
            if key.lower() not in HOP_HEADERS | {'content-encoding'}}
        return Response(content=response.content, status_code=response.status_code, headers=response_headers)
    except httpx.TimeoutException:
        raise HTTPException(504, '后端处理超时；秒杀请求请查询原记录或重试同一活动')
    except httpx.RequestError:
        raise HTTPException(502, '后端服务暂时不可用')
