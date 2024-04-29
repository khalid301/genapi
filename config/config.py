from pydantic_settings import BaseSettings


class GunicornConfig(BaseSettings):
    PORT: int = 8080
    HOST: str = "localhost"
    NUM_WORKERS: int = 1
    NUM_THREADS: int = 1
    TIMEOUT: int = 30
    LOG_LEVEL: str = "INFO"
    LOG_DEBUG: str = "DEBUG"
    LOG_ERROR: str = "ERROR"
    DB_DIALECT: str = "postgresql"
    DB_HOST: str = "103.41.206.233"
    DB_PORT: int = 5432
    DB_USERNAME: str = "postgres"
    DB_PASSWORD: str = "r00t_db"
    DB_NAME: str = "crm"

    class Config:
        env_prefix = "RG_DS_CHATBOT_API_GUNICORN_"


GUNICORN_CONFIG = GunicornConfig()
