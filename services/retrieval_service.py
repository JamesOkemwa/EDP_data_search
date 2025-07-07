from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from langchain.schema import Document
from qdrant_client import models
from vector_stores.qdrant_store import QdrantVectorStoreManager

@dataclass
class SearchResult:
    """Represents a search result with metadata"""
    title: Optional[str]
    content: str
    keywords: List[str]
    score: Optional[float] = None

    @classmethod
    def from_documents(cls, document: Document, score: Optional[float] = None) -> 'SearchResult':
        """Create a SearchResult from a Document"""
        return cls(
            title=document.metadata.get('title'),
            content=document.page_content,
            keywords=document.metadata.get('keywords', []),
            score=score
        )