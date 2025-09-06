# extra_requirements:
# tritonclient[all]
# attrdict
# git+https://github.com/hieupth/trism

"""
Search Service for Default Collection
Uses new API endpoint for searching instead of direct Qdrant client
"""

import logging
import numpy as np
import requests
from typing import List, Dict, Optional
from trism import TritonModel

logger = logging.getLogger(__name__)


class DefaultSearchService:
    """Search service using new API endpoint"""

    def __init__(self, config: Dict):
        self.config = config
        self.api_base_url = config.get(
            "api_base_url", "http://localhost:8000"
        )  # Add API base URL to config

    def generate_embedding(self, text: str) -> np.ndarray:
        """Generate embedding for query"""
        try:
            model = TritonModel(
                model=self.config["query_model"],
                version=self.config.get("triton_version", 1),
                url=self.config["triton_url"],
                grpc=self.config.get("triton_grpc", True),
            )

            model_input = np.array([[text.encode("utf-8")]], dtype=object)
            result = model.run(data=[model_input])
            output = list(result.values())[0]
            output = output.reshape(1, -1, 768)[:, 0]

            return np.array(output[0])

        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise

    def search(
        self,
        query: str,
        session_id: str,
        limit: int = 10,
        filters: Optional[Dict] = None,
        collection_name: Optional[str] = None,
    ) -> List[Dict]:
        """Search using new API endpoint"""
        try:
            # Generate query embedding
            query_embedding = self.generate_embedding(query)

            # Ensure correct format for API
            if not isinstance(query_embedding, list):
                query_embedding = query_embedding.tolist()

            if isinstance(query_embedding[0], list):
                query_embedding = query_embedding[0]

            #! TWO APIS: With Session id and not
            api_url = f"{self.api_base_url}/api/v1/chunks/session/search"

            if not collection_name:
                payload = {
                    "query_vector": query_embedding,
                    "limit": limit,
                    "filters": filters or {},
                }
            else:
                payload = {
                    "query_vector": query_embedding,
                    "limit": limit,
                    "filters": filters or {},
                    "collection_name": collection_name,
                }

            headers = {"Content-Type": "application/json"}

            logger.info(f"Sending search request to {api_url} with limit {limit}")

            # Make API request
            response = requests.post(api_url, json=payload, headers=headers)

            if response.status_code == 200:
                api_response = response.json()
                results = api_response.get("results", [])

                # Convert API response to the expected format (similar to Qdrant results)
                formatted_results = []
                for result in results:
                    formatted_result = {
                        "id": result.get("chunk_id"),
                        "score": result.get("similarity_score", 0.0),
                        "payload": {
                            "chunk_id": result.get("chunk_id"),
                            "document_id": result.get("document_id"),
                            "doc_title": result.get("document_title"),
                            "chunk_content": result.get("chunk_text"),
                            "source": result.get("source"),
                            "page": result.get("metadata", {}).get("page_number", 0),
                            "section": result.get("metadata", {}).get("section", ""),
                            **{
                                k: v
                                for k, v in result.get("metadata", {}).items()
                                if k not in ["page_number", "section"]
                            },
                        },
                    }
                    formatted_results.append(formatted_result)

                logger.info(
                    f"Found {len(formatted_results)} results for session {session_id}"
                )
                return formatted_results

            else:
                logger.error(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error during search: {e}")
            return []
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []


def main(
    query: str, config: Dict, session_id: str, tenant: Optional[str]
) -> List[Dict]:
    """Main search function using new API"""
    try:
        search_service = DefaultSearchService(config)
        results = search_service.search(
            query, session_id, limit=10, collection_name=tenant
        )
        return results
    except Exception as e:
        logger.error(f"Search service failed: {e}")
        return []
