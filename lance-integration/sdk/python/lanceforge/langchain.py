"""
LangChain VectorStore adapter for LanceForge.

Usage:
    from lanceforge.langchain import LanceForgeVectorStore
    from langchain_openai import OpenAIEmbeddings

    store = LanceForgeVectorStore(
        host="localhost:50050",
        table_name="documents",
        embedding=OpenAIEmbeddings(),
    )
    docs = store.similarity_search("what is LanceForge?", k=5)
"""

from typing import Any, Iterable, List, Optional

from lanceforge.client import LanceForgeClient

try:
    from langchain_core.documents import Document
    from langchain_core.embeddings import Embeddings
    from langchain_core.vectorstores import VectorStore
except ImportError:
    raise ImportError(
        "langchain-core is required for LanceForgeVectorStore. "
        "Install with: pip install langchain-core"
    )


class LanceForgeVectorStore(VectorStore):
    """LangChain VectorStore backed by a LanceForge distributed cluster.

    Args:
        host: Coordinator address (e.g., "localhost:50050")
        table_name: Name of the table to search
        embedding: LangChain Embeddings instance for text→vector
        api_key: Optional API key for authentication
        vector_column: Vector column name (default "vector")
        text_column: Text column name (default "text")
    """

    def __init__(
        self,
        host: str = "localhost:50050",
        table_name: str = "documents",
        embedding: Optional[Embeddings] = None,
        api_key: Optional[str] = None,
        vector_column: str = "vector",
        text_column: str = "text",
    ):
        self._client = LanceForgeClient(host=host, api_key=api_key)
        self._table_name = table_name
        self._embedding = embedding
        self._vector_column = vector_column
        self._text_column = text_column

    @property
    def embeddings(self) -> Optional[Embeddings]:
        return self._embedding

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[dict]] = None,
        **kwargs: Any,
    ) -> List[str]:
        """Add texts to the vector store.

        Embeds the texts using the configured embedding model and inserts
        into the LanceForge cluster.
        """
        import pyarrow as pa

        text_list = list(texts)
        if not text_list:
            return []

        if self._embedding is None:
            raise ValueError("Embedding model required for add_texts")

        vectors = self._embedding.embed_documents(text_list)
        dim = len(vectors[0])

        ids = list(range(len(text_list)))  # auto-generate IDs
        flat_vecs = [v for vec in vectors for v in vec]

        batch = pa.record_batch([
            pa.array(ids, type=pa.int64()),
            pa.array(text_list, type=pa.string()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(flat_vecs, type=pa.float32()), list_size=dim),
        ], names=["id", self._text_column, self._vector_column])

        self._client.insert(self._table_name, batch)
        return [str(i) for i in ids]

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        filter: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Document]:
        """Search for similar documents by text query.

        Embeds the query and performs ANN search against the cluster.
        """
        if self._embedding is None:
            raise ValueError("Embedding model required for similarity_search")

        query_vector = self._embedding.embed_query(query)
        return self.similarity_search_by_vector(query_vector, k=k, filter=filter)

    def similarity_search_by_vector(
        self,
        embedding: List[float],
        k: int = 4,
        filter: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Document]:
        """Search for similar documents by vector."""
        table = self._client.search(
            self._table_name,
            query_vector=embedding,
            k=k,
            filter=filter,
        )

        docs = []
        for i in range(table.num_rows):
            text = ""
            metadata = {}
            for col_name in table.column_names:
                val = table.column(col_name)[i].as_py()
                if col_name == self._text_column:
                    text = str(val)
                elif col_name not in (self._vector_column, "_distance", "_score"):
                    metadata[col_name] = val
                elif col_name in ("_distance", "_score"):
                    metadata[col_name] = val
            docs.append(Document(page_content=text, metadata=metadata))

        return docs

    @classmethod
    def from_texts(
        cls,
        texts: List[str],
        embedding: Embeddings,
        metadatas: Optional[List[dict]] = None,
        **kwargs: Any,
    ) -> "LanceForgeVectorStore":
        """Create a LanceForgeVectorStore from texts."""
        store = cls(embedding=embedding, **kwargs)
        store.add_texts(texts, metadatas=metadatas)
        return store
