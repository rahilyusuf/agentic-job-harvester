"""gateway — LiteLLM proxy client factory and embeddings helper.

All LLM and embedding calls in this project MUST go through this module.
No direct Vertex AI / Google GenAI API calls are permitted elsewhere.
"""

from gateway.client import GatewayClient, generate_embedding, get_instructor_client

__all__ = ["GatewayClient", "get_instructor_client", "generate_embedding"]
