"""Chunk Upload Service - Specialized Vector Database Insertion Module
Handles optimized batch uploading of document chunks to vector database with intelligent batching, retry logic, and comprehensive error handling."""

import requests
import logging
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    """Container for upload operation results."""

    success: bool
    total_chunks: int
    uploaded_chunks: int
    successful_batches: int
    failed_batches: int
    upload_time: float = 0.0
    errors: List[str] = None


class UploadError(Exception):
    """Custom exception for upload errors."""

    pass


@contextmanager
def upload_session(config: Dict):
    """Context manager for upload API session."""
    session = requests.Session()

    headers = {"Content-Type": "application/json"}
    if config.get("api_key"):
        headers["Authorization"] = f"Bearer {config['api_key']}"

    session.headers.update(headers)

    try:
        yield session
    finally:
        session.close()


class BatchUploader:
    """Optimized batch upload service with intelligent retry logic."""

    def __init__(
        self, session: requests.Session, config: Dict, tenant: Optional[str] = None
    ):
        self.session = session
        self.base_url = config["chunk_api_base_url"].rstrip("/")
        self.timeout = config.get("api_timeout", 30)
        self.tenant = tenant
        self.max_retries = config.get("max_upload_retries", 3)
        self.retry_delay = config.get("retry_delay_seconds", 1)

    def upload_chunks_batch(
        self, session_id: str, chunks_data: List[Dict], batch_size: int = 8
    ) -> UploadResult:
        """Upload chunks in optimized batches with comprehensive error handling."""
        total_chunks = len(chunks_data)
        successful_batches = 0
        failed_batches = 0
        total_uploaded = 0
        errors = []

        url = f"{self.base_url}/api/v1/chunks/session/{session_id}/chunks"

        logger.info(
            f"Starting batch upload: {total_chunks} chunks in batches of {batch_size}"
        )

        for i in range(0, total_chunks, batch_size):
            batch_number = (i // batch_size) + 1
            batch_chunks = chunks_data[i : i + batch_size]

            try:
                uploaded_count = self._upload_single_batch(
                    url, batch_chunks, batch_number, total_chunks
                )
                total_uploaded += uploaded_count
                successful_batches += 1

            except Exception as e:
                failed_batches += 1
                error_msg = f"Batch {batch_number} failed: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
                continue

            time.sleep(0.01)  # Brief pause between batches

        result = UploadResult(
            success=failed_batches == 0,
            total_chunks=total_chunks,
            uploaded_chunks=total_uploaded,
            successful_batches=successful_batches,
            failed_batches=failed_batches,
            errors=errors,
        )

        return result

    def _upload_single_batch(
        self, url: str, batch_chunks: List[Dict], batch_number: int, total_chunks: int
    ) -> int:
        """Upload a single batch with retry logic."""
        for attempt in range(self.max_retries):
            try:
                payload = {"chunks": batch_chunks}
                if self.tenant:
                    payload["collection_name"] = self.tenant

                response = self.session.post(url, json=payload, timeout=self.timeout)
                response.raise_for_status()

                result = response.json()
                chunks_processed = result.get("chunks_processed", len(batch_chunks))

                logger.info(
                    f"Batch {batch_number} successful: {chunks_processed} chunks uploaded"
                )
                return chunks_processed

            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise UploadError(
                        f"Failed to upload batch {batch_number} after {self.max_retries} attempts: {e}"
                    )

                logger.warning(
                    f"Upload attempt {attempt + 1} failed for batch {batch_number}: {e}"
                )
                time.sleep(self.retry_delay * (attempt + 1))


class ChunkUploadService:
    """Main chunk upload orchestrator."""

    def __init__(self, config: Dict, tenant: Optional[str] = None):
        self.config = config
        self.tenant = tenant

    def upload_document_chunks(self, chunking_data: Dict) -> Dict[str, Any]:
        """Upload all chunks from chunking results."""
        start_time = time.time()

        session_id = chunking_data.get("session_id")
        chunks_metadata = chunking_data.get("chunks_metadata", [])
        batch_size = chunking_data.get("batch_size", 8)

        if not session_id:
            raise UploadError("Missing session_id in chunking data")

        if not chunks_metadata:
            logger.warning("No chunks to upload")
            return {
                "success": True,
                "total_chunks": 0,
                "uploaded_chunks": 0,
                "upload_time": 0.0,
                "session_id": session_id,
                "message": "No chunks to upload",
            }

        logger.info(f"Uploading {len(chunks_metadata)} chunks to session {session_id}")

        with upload_session(self.config) as session:
            uploader = BatchUploader(session, self.config, self.tenant)
            result = uploader.upload_chunks_batch(
                session_id, chunks_metadata, batch_size
            )

        upload_time = time.time() - start_time
        result.upload_time = upload_time

        response = {
            "success": result.success,
            "total_chunks": result.total_chunks,
            "uploaded_chunks": result.uploaded_chunks,
            "successful_batches": result.successful_batches,
            "failed_batches": result.failed_batches,
            "upload_time": upload_time,
            "session_id": session_id,
            "tenant_name": self.tenant,
            "upload_rate_chunks_per_second": result.uploaded_chunks / upload_time
            if upload_time > 0
            else 0,
        }

        if result.errors:
            response["errors"] = result.errors

        logger.info(
            f"Upload completed: {result.uploaded_chunks}/{result.total_chunks} chunks "
            f"in {upload_time:.2f}s ({response['upload_rate_chunks_per_second']:.1f} chunks/s)"
        )

        return response


def main(chunking_data: Dict) -> Optional[str]:
    """
    Main chunk upload function with comprehensive error handling.

    Args:
        chunking_data: Results from document chunking

    Returns:
        Session ID if successful, None otherwise
    """
    try:
        config = {
            "chunk_api_base_url": chunking_data.get("config", {}).get(
                "chunk_api_base_url", ""
            ),
            "api_timeout": chunking_data.get("config", {}).get("api_timeout", 30),
            "api_key": chunking_data.get("config", {}).get("api_key"),
            "max_upload_retries": chunking_data.get("config", {}).get(
                "max_upload_retries", 3
            ),
        }

        tenant_name = chunking_data.get("tenant_name")
        session_id = chunking_data.get("session_id")

        upload_service = ChunkUploadService(config, tenant_name)
        result = upload_service.upload_document_chunks(chunking_data)

        if result["success"]:
            logger.info(f"Chunk upload completed successfully for session {session_id}")
            return session_id
        else:
            logger.error(f"Chunk upload failed for session {session_id}")
            return None

    except Exception as e:
        logger.error(f"Chunk upload service failed: {e}")
        raise
