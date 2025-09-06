"""Document Chunking Service - Specialized Text Processing Module
Handles document chunking with embedding generation via Context API, optimized for various file formats and processing efficiency."""

import requests
import logging
import time
import uuid
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@dataclass
class ChunkingResult:
    """Container for chunking operation results."""

    success: bool
    documents_processed: int
    total_chunks_created: int
    errors: List[str]
    processing_time: float = 0.0


class ChunkingError(Exception):
    """Custom exception for chunking errors."""

    pass


@contextmanager
def chunking_session(config: Dict):
    """Context manager for chunking API session."""
    session = requests.Session()

    headers = {"Accept": "application/json"}
    if config.get("api_key"):
        headers["Authorization"] = f"Bearer {config['api_key']}"

    session.headers.update(headers)

    try:
        yield session
    finally:
        session.close()


class ContextProcessor:
    """Optimized document processing via Context API."""

    def __init__(self, session: requests.Session, config: Dict):
        self.session = session
        self.base_url = config["context_api_base_url"].rstrip("/")
        self.timeout = config.get("api_timeout", 30)
        self.max_retries = config.get("max_chunking_retries", 2)

    def process_file_content(self, file_data: bytes, filename: str) -> List[Dict]:
        """Process file content via Context API with retry logic."""
        url = f"{self.base_url}/context"
        content_type = self._get_content_type(filename)

        for attempt in range(self.max_retries):
            try:
                files = {"file": (filename, file_data, content_type)}
                response = self.session.post(url, files=files, timeout=self.timeout)
                response.raise_for_status()

                data = response.json()

                if not data:
                    raise ChunkingError(
                        f"Empty response from Context API for {filename}"
                    )

                chunks = [
                    {
                        "chunk_id": item["id"],
                        "chunk_text": item["chunk"],
                        "embedding": item["emb"],
                    }
                    for item in data
                ]

                logger.info(f"Successfully chunked {filename}: {len(chunks)} chunks")
                return chunks

            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise ChunkingError(
                        f"Failed to process {filename} after {self.max_retries} attempts: {e}"
                    )

                logger.warning(
                    f"Chunking attempt {attempt + 1} failed for {filename}: {e}"
                )
                time.sleep(1 * (attempt + 1))

    @staticmethod
    def _get_content_type(filename: str) -> str:
        """Determine content type from filename extension."""
        ext = filename.lower().split(".")[-1]
        content_types = {
            "pdf": "application/pdf",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "doc": "application/msword",
            "txt": "text/plain",
            "md": "text/markdown",
            "html": "text/html",
            "json": "application/json",
            "xml": "application/xml",
        }
        return content_types.get(ext, "application/octet-stream")


class ChunkMetadataBuilder:
    """Builds optimized chunk metadata for storage."""

    def __init__(self):
        self.namespace = uuid.NAMESPACE_DNS

    def build_chunk_metadata(
        self, api_chunks: List[Dict], document_id: str, session_id: str
    ) -> List[Dict]:
        """Build comprehensive chunk metadata for storage."""
        chunks_metadata = []

        for i, api_chunk in enumerate(api_chunks):
            chunk_id = str(
                uuid.uuid5(
                    self.namespace, f"{document_id}_{i}_{api_chunk['chunk_text'][:50]}"
                )
            )

            metadata = {
                "chunk_id": chunk_id,
                "document_id": document_id,
                "document_title": f"document_{document_id}",
                "chunk_text": api_chunk["chunk_text"],
                "vector": api_chunk["embedding"],
                "chunk_index": i,
                "session_id": session_id,
                "metadata": {
                    "api_chunk_id": api_chunk["chunk_id"],
                    "chunk_length": len(api_chunk["chunk_text"]),
                    "processing_timestamp": time.time(),
                },
            }

            chunks_metadata.append(metadata)

        return chunks_metadata


class DocumentChunker:
    """Main document chunking orchestrator."""

    def __init__(self, config: Dict):
        self.config = config
        self.metadata_builder = ChunkMetadataBuilder()

    def chunk_documents(self, downloaded_data: Dict, session_id: str) -> Dict[str, Any]:
        """Process downloaded documents into chunks with metadata."""
        start_time = time.time()

        downloaded_content = downloaded_data.get("downloaded_content", {})

        all_chunks = []
        processed_count = 0
        errors = []

        logger.info(f"Starting chunking for {len(downloaded_content)} documents")

        with chunking_session(self.config) as session:
            context_processor = ContextProcessor(session, self.config)

            for document_id, file_content in downloaded_content.items():
                try:
                    filename = f"document_{document_id}.pdf"  # Default filename

                    api_chunks = context_processor.process_file_content(
                        file_content, filename
                    )

                    if not api_chunks:
                        raise ChunkingError(f"No chunks generated for {filename}")

                    chunks_metadata = self.metadata_builder.build_chunk_metadata(
                        api_chunks, document_id, session_id
                    )

                    all_chunks.extend(chunks_metadata)
                    processed_count += 1

                    logger.info(f"Processed {filename}: {len(chunks_metadata)} chunks")

                except Exception as e:
                    error_msg = f"Failed to chunk document {document_id}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)

        processing_time = time.time() - start_time

        result = {
            "success": processed_count > 0,
            "documents_processed": processed_count,
            "total_chunks_created": len(all_chunks),
            "processing_time": processing_time,
            "session_id": session_id,
            "tenant_name": downloaded_data.get("tenant_name"),
            "errors": errors,
            "chunks_metadata": all_chunks,
            "batch_size": downloaded_data.get("batch_size", 8),
        }

        logger.info(
            f"Chunking completed: {processed_count} docs, "
            f"{len(all_chunks)} chunks in {processing_time:.2f}s"
        )

        return result


def main(downloaded_data: Dict) -> Dict[str, Any]:
    """
    Main chunking function with comprehensive error handling.

    Args:
        downloaded_data: Downloaded content and metadata

    Returns:
        Chunking results with metadata
    """
    try:
        config = {
            "context_api_base_url": downloaded_data.get("config", {}).get(
                "context_api_base_url", ""
            ),
            "api_timeout": downloaded_data.get("config", {}).get("api_timeout", 30),
            "api_key": downloaded_data.get("config", {}).get("api_key"),
            "max_chunking_retries": downloaded_data.get("config", {}).get(
                "max_chunking_retries", 2
            ),
        }

        session_id = downloaded_data["session_id"]

        chunker = DocumentChunker(config)
        result = chunker.chunk_documents(downloaded_data, session_id)

        return result

    except Exception as e:
        logger.error(f"Document chunking failed: {e}")
        raise
