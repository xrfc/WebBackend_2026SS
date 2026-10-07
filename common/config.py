from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    SECRET_KEY: str = Field(min_length=32)
    ALGORITHM: str = 'HS256'
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(60, ge=1, le=1440)
    MYSQL_HOST: str = 'localhost'
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = 'webbackend'
    MYSQL_PASSWORD: str = ''
    MYSQL_DATABASE: str = 'webbackend'
    DATABASE_URL: str = ''
    REDIS_HOST: str = 'localhost'
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ''
    REDIS_URL: str = ''
    RABBITMQ_HOST: str = 'localhost'
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = 'webbackend'
    RABBITMQ_PASS: str = ''
    LLM_API_KEY: str = ''
    LLM_BASE_URL: str = 'https://api.deepseek.com'
    LLM_MODEL: str = 'deepseek-chat'
    LLM_TIMEOUT_SECONDS: float = Field(15, gt=0, le=25)
    USER_SERVICE_URL: str = 'http://localhost:8001'
    PRODUCT_SERVICE_URL: str = 'http://localhost:8002'
    ORDER_SERVICE_URL: str = 'http://localhost:8003'
    SECKILL_SERVICE_URL: str = 'http://localhost:8004'
    AI_SERVICE_URL: str = 'http://localhost:8005'
    CORS_ORIGINS: str = 'http://localhost:8000,http://127.0.0.1:8000'
    MAX_BODY_BYTES: int = Field(65536, ge=1024, le=1048576)
    MAX_OUTBOX_LENGTH: int = Field(10000, ge=1, le=1000000)
    REQUEST_RETENTION_SECONDS: int = Field(604800, ge=86400)
    MQ_MAX_RETRIES: int = Field(3, ge=0, le=10)
    MQ_RETRY_DELAY_MS: int = Field(5000, ge=100, le=60000)
    MQ_PREFETCH: int = Field(16, ge=1, le=64)
    SECKILL_RATE_LIMIT: int = Field(30, ge=1, le=1000)
    ADMIN_USERNAME: str = 'admin'
    ADMIN_PASSWORD: str = ''

    @field_validator('SECRET_KEY')
    @classmethod
    def reject_example_key(cls, value):
        if value.lower().startswith(('change-me', 'webbackend-secret')):
            raise ValueError('Generate a random SECRET_KEY (scripts/deploy.py does this automatically)')
        return value


# Unit tests must explicitly supply a secret, just like deployments.
settings = Settings()
