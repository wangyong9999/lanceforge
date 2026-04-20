"""
LanceForge Python SDK — distributed multimodal retrieval client.

Sync (default):
    from lanceforge import LanceForgeClient
    client = LanceForgeClient("localhost:50050")
    results = client.search("products", query_vector=[0.1, 0.2, ...], k=10)
    client.insert("products", data)
    client.delete("products", filter="id > 100")

Async (FastAPI / LangChain / asyncio-native apps, 0.2.0-beta.3+):
    from lanceforge import AsyncLanceForgeClient
    async with AsyncLanceForgeClient("localhost:50050") as client:
        results = await client.search("products", query_vector=[...], k=10)
        await client.insert("products", data)
"""

from lanceforge.client import LanceForgeClient
from lanceforge.async_client import AsyncLanceForgeClient

__all__ = ["LanceForgeClient", "AsyncLanceForgeClient"]
__version__ = "0.2.0-beta.3"
