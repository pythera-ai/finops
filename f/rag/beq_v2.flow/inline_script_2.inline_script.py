"""Context Service - Optimized Context Processing and Formatting
Handles intelligent context selection, processing and formatting for optimal LLM consumption with performance optimizations."""

import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ContextItem:
    """Structured context item with metadata."""

    id: str
    score: float
    text: str
    metadata: Dict[str, Any]
    source: str


class ContextProcessor:
    """Optimized context processing service."""

    def __init__(self, max_contexts: int = 5, min_score_threshold: float = 0.0):
        self.max_contexts = max_contexts
        self.min_score_threshold = min_score_threshold

    def process_search_results(self, search_results: List[Dict]) -> List[ContextItem]:
        """
        Convert search results to structured context items with validation.

        Args:
            search_results: Raw search results

        Returns:
            List of validated context items
        """
        contexts = []

        for result in search_results:
            try:
                context_item = self._extract_context_item(result)
                if context_item and self._validate_context_item(context_item):
                    contexts.append(context_item)
            except Exception as e:
                logger.warning(f"Failed to process search result: {e}")
                continue

        logger.info(
            f"Processed {len(contexts)} valid contexts from {len(search_results)} results"
        )
        return contexts

    def _extract_context_item(self, result: Dict) -> Optional[ContextItem]:
        """Extract context item from search result."""
        if not isinstance(result, dict):
            return None

        context_id = result.get("id", "unknown")
        score = result.get("score", 0.0)

        # Extract text content with fallbacks
        payload = result.get("payload", {})
        text = (
            payload.get("chunk_content")
            or payload.get("text")
            or payload.get("chunk_text")
            or ""
        )

        if not text.strip():
            return None

        source = payload.get("source", "search")

        return ContextItem(
            id=context_id,
            score=score,
            text=text.strip(),
            metadata=payload,
            source=source,
        )

    def _validate_context_item(self, context_item: ContextItem) -> bool:
        """Validate context item quality."""
        # Check minimum score threshold
        if context_item.score < self.min_score_threshold:
            return False

        # Check text quality
        if len(context_item.text) < 10:  # Minimum meaningful length
            return False

        return True

    def select_top_contexts(self, contexts: List[ContextItem]) -> List[ContextItem]:
        """
        Select top contexts using intelligent ranking.

        Args:
            contexts: List of context items

        Returns:
            Selected top contexts
        """
        if not contexts:
            return []

        # Sort by score (descending) with stability
        sorted_contexts = sorted(
            contexts, key=lambda x: (x.score, len(x.text)), reverse=True
        )

        # Select top N contexts
        selected = sorted_contexts[: self.max_contexts]

        logger.info(
            f"Selected {len(selected)} contexts from {len(contexts)} candidates"
        )
        return selected

    def format_contexts_for_llm(self, contexts: List[ContextItem]) -> str:
        """
        Format contexts for optimal LLM consumption.

        Args:
            contexts: List of context items to format

        Returns:
            Formatted context string
        """
        if not contexts:
            return ""

        formatted_parts = ["=== REFERENCE INFORMATION ==="]

        for i, context in enumerate(contexts, 1):
            # Extract document identifier
            doc_identifier = self._extract_document_identifier(context)

            # Format context entry
            formatted_parts.extend(
                [f"\nContext {i} (from {doc_identifier}):", context.text]
            )

        formatted_parts.append("\n=== END REFERENCE INFORMATION ===")

        return "\n".join(formatted_parts)

    def _extract_document_identifier(self, context: ContextItem) -> str:
        """Extract meaningful document identifier from context."""
        metadata = context.metadata

        # Try multiple possible identifier fields
        identifier = (
            metadata.get("filename")
            or metadata.get("doc_title")
            or metadata.get("document_title")
            or metadata.get("document_name")
            or f"Document {context.id[:8]}"
        )

        return str(identifier)


def main(search_results: List[Dict], upload_mode: bool) -> Tuple[List[Dict], str]:
    """
    Main context processing function with comprehensive handling.

    Args:
        search_results: Search results to process
        upload_mode: Whether in upload mode (affects processing)

    Returns:
        Tuple of (processed contexts, formatted context string)
    """
    try:
        # Initialize processor with adaptive parameters
        max_contexts = 7 if upload_mode else 5
        processor = ContextProcessor(max_contexts=max_contexts)

        # Process search results
        contexts = processor.process_search_results(search_results)

        # Select top contexts
        selected_contexts = processor.select_top_contexts(contexts)

        # Format for LLM
        formatted_context = processor.format_contexts_for_llm(selected_contexts)

        # Convert back to dict format for compatibility
        context_dicts = []
        for context in selected_contexts:
            context_dict = {
                "id": context.id,
                "score": context.score,
                "payload": {
                    "text": context.text,
                    "source": context.source,
                    **context.metadata,
                },
            }
            context_dicts.append(context_dict)

        logger.info(
            f"Context processing completed: {len(context_dicts)} contexts prepared"
        )

        return context_dicts, formatted_context

    except Exception as e:
        logger.error(f"Context service failed: {e}")
        return [], ""
