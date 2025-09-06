"""Input Validation Module for Document Processing Flow
Validates and prepares input parameters for the processing pipeline."""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


def validate_documents_list(documents: List[str]) -> List[str]:
    """Validate and clean document IDs list."""
    if not documents:
        raise ValueError("No documents provided for processing")

    # Remove duplicates and empty strings
    clean_docs = list(set(doc.strip() for doc in documents if doc and doc.strip()))

    if not clean_docs:
        raise ValueError("No valid document IDs found")

    logger.info(f"Validated {len(clean_docs)} document IDs for processing")
    return clean_docs


def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate configuration parameters."""
    required_urls = [
        "docman_api_base_url",
        "context_api_base_url",
        "chunk_api_base_url",
    ]

    for url_key in required_urls:
        if not config.get(url_key):
            raise ValueError(f"Missing required configuration: {url_key}")

    # Set default values
    config.setdefault("api_timeout", 30)
    config.setdefault("max_download_retries", 3)
    config.setdefault("max_chunking_retries", 2)
    config.setdefault("max_upload_retries", 3)
    config.setdefault("batch_size_insert", 8)

    return config


def main(
    upload_documents: List[str],
    session_id: str,
    config: Dict[str, Any],
    tenant_name: Optional[str] = None,
    batch_size_insert: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Main input validation function.

    Args:
        upload_documents: List of document IDs to process
        session_id: Session identifier
        config: System configuration
        tenant_name: Tenant identifier
        batch_size_insert: Batch size for uploads

    Returns:
        Validated input parameters
    """
    try:
        # Validate inputs
        if not session_id or not session_id.strip():
            raise ValueError("Session ID is required")

        clean_documents = validate_documents_list(upload_documents)
        validated_config = validate_config(config)

        # Override batch size if provided
        if batch_size_insert:
            validated_config["batch_size_insert"] = batch_size_insert

        result = {
            "session_id": session_id.strip(),
            "document_ids": clean_documents,
            "config": validated_config,
            "tenant_name": tenant_name or "default",
            "batch_size": validated_config["batch_size_insert"],
            "processing_mode": "batch",
            "total_documents": len(clean_documents),
        }

        logger.info(
            f"Input validation completed: {len(clean_documents)} documents for session {session_id}"
        )
        return result

    except Exception as e:
        logger.error(f"Input validation failed: {e}")
        raise
