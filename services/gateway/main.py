"""API网关 - 统一入口路由、JWT鉴权放行、CORS"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import httpx
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from common.config import settings
from common.auth import decode_token

# ==================== 服务路由映射 ====================

# 需要JWT鉴权的路径前缀
PROTECTED_PREFIXES = [
    "/orders",
    "/seckill",
    "/ai",
]

# 公开路径（不需要鉴权）
PUBLIC_PREFIXES = [
    "/register",
    "/login",
    "/health",
    "/products",
    "/docs",
    "/openapi.json",
    "/redoc",
]

# 路由 -> 目标服务映射
ROUTE_MAP = {
    "/register": settings.USER_SERVICE_URL,
    "/login": settings.USER_SERVICE_URL,
    "/logout": settings.USER_SERVICE_URL,
    "/me": settings.USER_SERVICE_URL,
    "/products": settings.PRODUCT_SERVICE_URL,
    "/orders": settings.ORDER_SERVICE_URL,
    "/seckill": settings.SECKILL_SERVICE_URL,
    "/ai": settings.AI_SERVICE_URL,
    "/ws": settings.ORDER_SERVICE_URL,  # WebSocket也走订单服务
}


def get_target_service(path: str) -> str:
    """根据请求路径匹配目标服务URL"""
    for prefix, target in ROUTE_MAP.items():
        if path.startswith(prefix):
            return target
    return settings.USER_SERVICE_URL  # 默认路由到用户服务


def is_protected(path: str) -> bool:
    """判断路径是否需要JWT鉴权"""
    for prefix in PROTECTED_PREFIXES:
        if path.startswith(prefix):
            # 检查是否有公开前缀覆盖
            for pub in PUBLIC_PREFIXES:
                if path.startswith(pub):
                    return False
            return True
    return False


# ==================== FastAPI 网关应用 ====================

app = FastAPI(title="WebBackend API Gateway", version="1.0.0",
              description="AI驱动的高并发秒杀与智能电商系统 - 统一API网关")

# CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def gateway_proxy(request: Request, path: str):
    """核心网关代理逻辑 - 鉴权 + 路由转发"""
    request_path = "/" + path

    # 1. JWT鉴权检查
    if is_protected(request_path):
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"code": 401, "message": "未提供认证Token", "data": None}
            )
        token = auth_header.replace("Bearer ", "")
        try:
            decode_token(token)
        except HTTPException:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"code": 401, "message": "Token无效或已过期", "data": None}
            )

    # 2. 确定目标服务
    target_url = get_target_service(request_path)
    full_url = f"{target_url}{request_path}"

    # 如果有查询参数，附加到URL
    if request.query_params:
        full_url += f"?{request.query_params}"

    # 3. 转发请求
    body = await request.body() if request.method in ("POST", "PUT", "PATCH") else None
    headers = dict(request.headers)
    headers.pop("host", None)  # 移除原始host头

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=request.method,
                url=full_url,
                headers=headers,
                content=body,
            )
            return JSONResponse(
                status_code=response.status_code,
                content=response.json(),
            )
    except httpx.ConnectError:
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={"code": 502, "message": f"后端服务不可用: {target_url}", "data": None}
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"code": 500, "message": f"网关转发异常: {str(e)}", "data": None}
        )


@app.get("/health")
async def health():
    """网关健康检查"""
    return {"status": "ok", "gateway": "WebBackend API Gateway v1.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
