from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SECRET_KEY: str = "webbackend-secret-key-2026"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = "root123"
    MYSQL_DATABASE: str = "webbackend"

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "admin"
    RABBITMQ_PASS: str = "admin123"

    LLM_API_KEY: str = "sk-your-api-key-here"
    LLM_BASE_URL: str = "https://api.openai.com/v1"
    LLM_MODEL: str = "gpt-3.5-turbo"

    USER_SERVICE_URL: str = "http://localhost:8001"
    PRODUCT_SERVICE_URL: str = "http://localhost:8002"
    ORDER_SERVICE_URL: str = "http://localhost:8003"
    SECKILL_SERVICE_URL: str = "http://localhost:8004"
    AI_SERVICE_URL: str = "http://localhost:8005"

    class Config:
        env_file = ".env"


settings = Settings()
