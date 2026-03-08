from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        populate_by_name=True,
        case_sensitive=False,
    )

    # xAI / Grok API
    xai_api_key: str = Field(default="", alias="XAI_API_KEY")
    xai_base_url: str = Field(default="https://api.x.ai/v1", alias="XAI_BASE_URL")
    grok_model: str = Field(default="grok-3-beta", alias="GROK_MODEL")

    # Gmail OAuth2
    google_client_id: str = Field(default="", alias="GOOGLE_CLIENT_ID")
    google_client_secret: str = Field(default="", alias="GOOGLE_CLIENT_SECRET")
    google_refresh_token: str = Field(default="", alias="GOOGLE_REFRESH_TOKEN")

    # ContractGHOST Workspace API
    ghost_api_url: str = Field(default="", alias="GHOST_API_URL")
    ghost_api_key: str = Field(default="", alias="GHOST_API_KEY")

    # Social Media
    threads_user_id: str = Field(default="", alias="THREADS_USER_ID")
    threads_access_token: str = Field(default="", alias="THREADS_ACCESS_TOKEN")
    tiktok_access_token: str = Field(default="", alias="TIKTOK_ACCESS_TOKEN")

    # App Settings
    autonomy_mode: bool = Field(default=False, alias="AUTONOMY_MODE")
    threads_handle: str = Field(default="@TheMrFlen", alias="THREADS_HANDLE")

    # Database
    database_url: str = Field(
        default="sqlite+aiosqlite:///./align.db", alias="DATABASE_URL"
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
