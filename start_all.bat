@echo off
echo ============================================
echo   WebBackend - AI驱动的高并发秒杀智能电商
echo ============================================
echo.
echo [1/3] 停止本地MySQL(避免端口冲突)...
net stop MySQL 2>nul
sc config MySQL start=disabled 2>nul
echo.

echo [2/3] 启动Docker中间件 (MySQL+Redis+RabbitMQ)...
docker compose -f "%~dp0docker-compose.yml" up -d
echo   等待MySQL就绪(最多60秒)...
docker exec webbackend_mysql mysqladmin ping -h localhost -p"Six666666." --silent 2>nul
if errorlevel 1 (
    timeout /t 30 /nobreak >nul
    docker exec webbackend_mysql mysqladmin ping -h localhost -p"Six666666." --silent 2>nul
)
echo   中间件已就绪!
echo.

echo [3/3] 启动6个微服务...
echo.
start "Gateway(8000)"       cmd /k "uvicorn services.gateway.main:app       --host 0.0.0.0 --port 8000 --reload"
timeout /t 2 /nobreak >nul
start "UserService(8001)"   cmd /k "uvicorn services.user_service.main:app   --host 0.0.0.0 --port 8001 --reload"
timeout /t 2 /nobreak >nul
start "ProductService(8002)" cmd /k "uvicorn services.product_service.main:app --host 0.0.0.0 --port 8002 --reload"
timeout /t 2 /nobreak >nul
start "OrderService(8003)"  cmd /k "uvicorn services.order_service.main:app  --host 0.0.0.0 --port 8003 --reload"
timeout /t 2 /nobreak >nul
start "SeckillService(8004)" cmd /k "uvicorn services.seckill_service.main:app --host 0.0.0.0 --port 8004 --reload"
timeout /t 2 /nobreak >nul
start "AIService(8005)"     cmd /k "uvicorn services.ai_service.main:app     --host 0.0.0.0 --port 8005 --reload"

echo.
echo ============================================
echo  全部就绪! (Docker MySQL + Redis + RabbitMQ)
echo.
echo  网关:     http://localhost:8000/docs
echo  RabbitMQ: http://localhost:15672 (admin/admin123)
echo ============================================
echo.
echo  运行测试: python test_integration.py
echo.
pause
