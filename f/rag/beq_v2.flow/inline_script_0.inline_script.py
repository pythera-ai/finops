"""
Configuration validation and setup module
Centralizes all configuration management and environment setup
"""

import os
import json
import logging
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional
import wmill

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    """API-based system configuration with validation."""

    # Core API endpoints
    rag_api_base_url: str
    docman_api_base_url: str

    # Authentication and connection
    api_key: Optional[str] = None
    api_timeout: int = 30

    # Processing parameters
    chunk_size: int = 500
    chunk_overlap: int = 100
    search_limit: int = 10

    # LLM configuration
    llm_model: str = "gemma3-12b-4bit"
    temperature: float = 0.6
    max_tokens: int = 5000
    top_p: float = 0.6

    def validate(self) -> None:
        """Validate required configuration fields."""
        required = ["rag_api_base_url", "docman_api_base_url"]

        for field in required:
            value = getattr(self, field)
            if not value:
                raise ValueError(f"Missing required config: {field}")

            if not value.startswith(("http://", "https://")):
                raise ValueError(f"Invalid URL format for {field}: {value}")

    def derive_service_urls(self) -> Dict[str, str]:
        """Generate service-specific URLs from base URLs."""
        rag_base = self.rag_api_base_url.rstrip("/")
        docman_base = self.docman_api_base_url.rstrip("/")

        return {
            # Core API bases
            "api_base_url": rag_base,
            "rag_api_base_url": rag_base,
            "docman_api_base_url": docman_base,
            # Service-specific endpoints
            "document_api_base_url": docman_base,
            "chunk_api_base_url": docman_base,
            "search_api_base_url": docman_base,
            "embedding_api_base_url": rag_base,
            "context_api_base_url": rag_base,
        }


class SecretManager:
    """Handles secure credential management with caching."""

    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}

    def get_secret(self, key: str) -> Dict[str, Any]:
        """Retrieve and cache secret data."""
        if key not in self._cache:
            try:
                secret_data = json.loads(wmill.get_variable(key))
                self._cache[key] = secret_data
                logger.info(f"Successfully loaded secret: {key}")
            except Exception as e:
                logger.error(f"Failed to load secret {key}: {e}")
                raise

        return self._cache[key]

    def setup_environment(self) -> None:
        """Configure environment variables from secrets."""
        try:
            google_secrets = self.get_secret("u/hungnguyen131002/googe_api_key")
            os.environ["GOOGLE_API_KEY"] = google_secrets.get("GOOGLE_API_KEY", "")
            logger.info("Environment setup completed")
        except Exception as e:
            logger.error(f"Environment setup failed: {e}")
            raise


def main(raw_configs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main configuration validation and setup function.

    Args:
        raw_configs: Raw configuration dictionary from flow input

    Returns:
        Validated configuration with derived service URLs
    """
    try:
        api_config_data = raw_configs

        # Create and validate configuration
        config = APIConfig(
            rag_api_base_url=api_config_data.get("rag_url", ""),
            docman_api_base_url=api_config_data.get("docman_url", ""),
            api_key=api_config_data.get("api_key"),
            api_timeout=api_config_data.get("timeout", 30),
        )

        config.validate()

        # Setup environment and secrets
        secret_manager = SecretManager()
        secret_manager.setup_environment()

        # Build final configuration
        result_config = asdict(config)
        result_config.update(config.derive_service_urls())

        logger.info("Configuration validation completed successfully")
        logger.info(f"RAG API: {config.rag_api_base_url}")
        logger.info(f"DocMan API: {config.docman_api_base_url}")

        return result_config

    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        raise
