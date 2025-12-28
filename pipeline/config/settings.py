"""
Configuration Settings
Loads environment variables and provides configuration for the application
"""
import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Snowflake Configuration (VPN Side)
    snowflake_user: str = Field(..., env="SNOWFLAKE_USER")
    snowflake_account: str = Field(..., env="SNOWFLAKE_ACCOUNT")
    snowflake_warehouse: str = Field(..., env="SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = Field(..., env="SNOWFLAKE_DATABASE")
    snowflake_schema: str = Field(..., env="SNOWFLAKE_SCHEMA")
    snowflake_role: str = Field(..., env="SNOWFLAKE_ROLE")  # Required for SSO
    
    # Optional Snowflake parameters
    snowflake_region: Optional[str] = Field(None, env="SNOWFLAKE_REGION")
    
    # Snowflake Authentication
    snowflake_auth_method: str = Field("sso", env="SNOWFLAKE_AUTH_METHOD")  # sso, password, key_pair, oauth
    snowflake_password: Optional[str] = Field(None, env="SNOWFLAKE_PASSWORD")
    snowflake_private_key_path: Optional[str] = Field(None, env="SNOWFLAKE_PRIVATE_KEY_PATH")
    snowflake_private_key_passphrase: Optional[str] = Field(None, env="SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    
    # PostgreSQL Configuration (External Side)
    postgres_host: str = Field(..., env="POSTGRES_HOST")
    postgres_port: int = Field(5432, env="POSTGRES_PORT")
    postgres_database: str = Field(..., env="POSTGRES_DATABASE")
    postgres_user: str = Field(..., env="POSTGRES_USER")
    postgres_password: str = Field(..., env="POSTGRES_PASSWORD")
    
    # Redis Configuration (for Celery)
    redis_url: str = Field("redis://localhost:6379/0", env="REDIS_URL")
    
    # API Configuration
    api_secret_key: str = Field(..., env="API_SECRET_KEY")
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8000, env="API_PORT")
    
    # Logging Configuration
    log_level: str = Field("INFO", env="LOG_LEVEL")
    
    # Environment
    environment: str = Field("development", env="ENVIRONMENT")
    
    # Data Export/Import Settings
    export_base_dir: str = Field("exports", env="EXPORT_BASE_DIR")
    import_base_dir: str = Field("imports", env="IMPORT_BASE_DIR")
    chunk_size: int = Field(100000, env="CHUNK_SIZE")
    compression_type: str = Field("zstd", env="COMPRESSION_TYPE")
    compression_level: int = Field(3, env="COMPRESSION_LEVEL")
    encryption_iterations: int = Field(100000, env="KEY_DERIVATION_ITERATIONS")
    obfuscate_names: bool = Field(False, env="OBFUSCATE_NAMES")  # Enable name obfuscation
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields in .env file

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()

# Convenience functions for specific connection strings
def get_snowflake_connection_params() -> dict:
    """
    Get Snowflake connection parameters as dictionary
    Based on successful YAML connection practice
    """
    settings = get_settings()
    
    # Base connection parameters (matching successful YAML practice)
    params = {
        "account": settings.snowflake_account,
        "user": settings.snowflake_user,
        "role": settings.snowflake_role,
        "warehouse": settings.snowflake_warehouse,
        "database": settings.snowflake_database,
        "schema": settings.snowflake_schema,
    }
    
    # Add authentication-specific parameters
    if settings.snowflake_auth_method == "sso":
        params["authenticator"] = "externalbrowser"
        # SSO will open a browser window for authentication
    elif settings.snowflake_auth_method == "password":
        if not settings.snowflake_password:
            raise ValueError("SNOWFLAKE_PASSWORD required when using password authentication")
        params["password"] = settings.snowflake_password
    elif settings.snowflake_auth_method == "key_pair":
        if not settings.snowflake_private_key_path:
            raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH required when using key pair authentication")
        params["private_key_file"] = settings.snowflake_private_key_path
        if settings.snowflake_private_key_passphrase:
            params["private_key_file_pwd"] = settings.snowflake_private_key_passphrase
    elif settings.snowflake_auth_method == "oauth":
        raise NotImplementedError("OAuth authentication not yet implemented")
    else:
        raise ValueError(f"Unsupported authentication method: {settings.snowflake_auth_method}")
    
    # Add optional parameters for better connection management
    # These improve connection stability and session management
    params["client_session_keep_alive"] = True  # Keep session alive
    params["login_timeout"] = 120  # 2 minutes for SSO login
    params["network_timeout"] = 30  # 30 seconds for network operations
    
    return params

def get_postgres_connection_string() -> str:
    """Get PostgreSQL connection string"""
    settings = get_settings()
    return (
        f"postgresql://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_database}"
    )

def get_postgres_connection_params() -> dict:
    """Get PostgreSQL connection parameters as dictionary"""
    settings = get_settings()
    return {
        "host": settings.postgres_host,
        "port": settings.postgres_port,
        "database": settings.postgres_database,
        "user": settings.postgres_user,
        "password": settings.postgres_password
    }