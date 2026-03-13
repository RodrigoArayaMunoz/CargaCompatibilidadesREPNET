from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    frontend_url: str = "http://localhost:5173"
    upload_dir: str = "uploads"

    redis_url: str = "redis://localhost:6379/0"
    tokens_file: str = "tokens.json"

    ml_client_id: str | None = None
    ml_client_secret: str | None = None
    ml_redirect_uri: str | None = None

    ml_auth_url: str = "https://auth.mercadolibre.cl/authorization"
    ml_token_url: str = "https://api.mercadolibre.com/oauth/token"
    ml_me_url: str = "https://api.mercadolibre.com/users/me"
    ml_api_base: str = "https://api.mercadolibre.com"
    ml_domain_id: str = "MLC-CARS_AND_VANS_FOR_COMPATIBILITIES"
    ml_site_id: str = "MLC"

    max_row_concurrency: int = 8
    ml_http_max_connections: int = 100
    ml_http_max_keepalive: int = 20
    ml_http_timeout: int = 60
    ml_retry_attempts: int = 5


settings = Settings()