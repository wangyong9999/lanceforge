"""
LanceForge Python SDK — distributed multimodal retrieval client.

Usage:
    from lanceforge import LanceForgeClient

    client = LanceForgeClient("localhost:50050")
    results = client.search("products", query_vector=[0.1, 0.2, ...], k=10)
    client.insert("products", data)
    client.delete("products", filter="id > 100")
"""

from lanceforge.client import LanceForgeClient

__all__ = ["LanceForgeClient"]
__version__ = "0.1.0"
