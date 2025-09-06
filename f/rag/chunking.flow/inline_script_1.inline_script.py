"""Document Downloader Service - Specialized File Download Module
Handles efficient document downloading with retry logic, validation, and optimized resource management."""

import requests
import logging
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    """Container for download operation results."""

    success: bool
    downloaded_files: int
    total_size_bytes: int
    errors: List[str]
    download_time: float = 0.0


class DownloadError(Exception):
    """Custom exception for download errors."""

    pass


@contextmanager
def download_session(config: Dict):
    """Context manager for download API session."""
    session = requests.Session()

    headers = {"Accept": "*/*"}
    if config.get("api_key"):
        headers["Authorization"] = f"Bearer {config['api_key']}"

    session.headers.update(headers)

    try:
        yield session
    finally:
        session.close()


class DocumentDownloader:
    """Optimized document download service with retry and validation."""

    def __init__(self, session: requests.Session, config: Dict):
        self.session = session
        self.base_url = config["docman_api_base_url"].rstrip("/")
        self.timeout = config.get("api_timeout", 30)
        self.max_retries = config.get("max_download_retries", 3)
        self.retry_delay = config.get("retry_delay_seconds", 1)

    def download_document(self, document_id: str) -> bytes:
        """Download single document with retry logic."""
        url = f"{self.base_url}/api/v1/documents/{document_id}/download"

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()

                content = response.content
                if not content:
                    raise DownloadError(f"Empty content for document {document_id}")

                logger.info(f"Downloaded document {document_id}: {len(content)} bytes")
                return content

            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise DownloadError(
                        f"Failed to download {document_id} after {self.max_retries} attempts: {e}"
                    )

                logger.warning(
                    f"Download attempt {attempt + 1} failed for {document_id}: {e}"
                )
                time.sleep(self.retry_delay * (attempt + 1))

    def download_multiple_documents(self, document_ids: List[str]) -> Dict[str, bytes]:
        """Download multiple documents efficiently."""
        downloaded_content = {}

        for document_id in document_ids:
            try:
                content = self.download_document(document_id)
                downloaded_content[document_id] = content
            except Exception as e:
                logger.error(f"Failed to download document {document_id}: {e}")
                continue

        return downloaded_content


def main(validated_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main document download function.

    Args:
        validated_input: Validated input parameters

    Returns:
        Download results with content mapping
    """
    start_time = time.time()

    try:
        document_ids = validated_input["document_ids"]
        config = validated_input["config"]
        session_id = validated_input["session_id"]
        tenant_name = validated_input["tenant_name"]

        with download_session(config) as session:
            downloader = DocumentDownloader(session, config)
            downloaded_content = downloader.download_multiple_documents(document_ids)

        download_time = time.time() - start_time
        total_size = sum(len(content) for content in downloaded_content.values())

        # Create documents metadata for successful downloads
        documents_metadata = {}
        for doc_id in downloaded_content.keys():
            documents_metadata[doc_id] = {
                "document_id": doc_id,
                "filename": f"document_{doc_id}",  # Will be enriched by document service if needed
                "file_type": "unknown",  # Will be detected during chunking
                "file_size": len(downloaded_content[doc_id]),
            }

        result = {
            "success": len(downloaded_content) > 0,
            "downloaded_files": len(downloaded_content),
            "total_size_bytes": total_size,
            "download_time": download_time,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "downloaded_content": downloaded_content,
            "documents_metadata": documents_metadata,
            "batch_size": validated_input["batch_size"],
        }

        logger.info(
            f"Download completed: {len(downloaded_content)} files, "
            f"{total_size} bytes in {download_time:.2f}s"
        )

        return result

    except Exception as e:
        logger.error(f"Document download failed: {e}")
        raise
