import os
import logging
from typing import List, Optional, Set
from dataclasses import dataclass
from langchain.schema import Document
from qdrant_client import models
from dotenv import load_dotenv
from vector_stores.qdrant_store import QdrantVectorStoreManager

@dataclass
class SearchResult:
    """Represents a search result with metadata"""
    dataset_id: str
    content: str
    score: Optional[float] = None
    metadata: Optional[dict] = None

    @classmethod
    def from_documents(cls, document: Document, score: Optional[float] = None) -> 'SearchResult':
        """Create a SearchResult from a Document"""
        return cls(
            dataset_id=document.metadata.get('dataset_id', ''),
            content=document.page_content,
            score=score,
            metadata=document.metadata
        )
    

class DatasetRetrievalService:
    """Service for retriving datasets from a vector store"""

    def __init__(self):
        load_dotenv()
        self.logger = logging.getLogger(__name__)

        self.vector_store_manager = QdrantVectorStoreManager(
            host=os.getenv("QDRANT_HOST"),
            port=int(os.getenv("QDRANT_PORT")),
            collection_name=os.getenv("COLLECTION_NAME")
        )
        self._initialized = False

    def initialize(self):
        """Initialize the retrival service"""
        if not self._initialized:
            self.vector_store_manager.initialize()
            self._initialized = True
            self.logger.info("DatasetRetrievalService initialized successfully.")

    def search_by_dataset_ids(self, query: str, dataset_ids: List[str], max_results: int = 5, include_scores: bool = False) -> List[SearchResult]:

        """Perfom semantic similarity search limited to specific vector embeddings by their dataset_ids"""
        if not self._initialized:
                self.initialize()

        filter_criteria = self._build_dataset_id_filter(dataset_ids)
        self.logger.info("Searching vector store with dataset IDs...")

        try:
            if include_scores:
                documents_with_scores = self.vector_store_manager.similarity_search_with_score(
                    query=query,
                    k=max_results,
                    filter_criteria=filter_criteria
                )

                return [
                    SearchResult.from_documents(doc, score)
                    for doc, score in documents_with_scores
                ]
            else:
                documents = self.vector_store_manager.similarity_search(
                    query=query,
                    k=max_results,
                    filter_criteria=filter_criteria
                )

                return [SearchResult.from_documents(doc) for doc in documents]
            
        except Exception as e:
            self.logger.error(f"Error during search: {e}")
            return []

    def search_all_embeddings(self, query: str, max_results: int = 5, include_scores: bool = False) -> List[SearchResult]:
        """Perform semantic similarity search across all vector embeddings: no filtering"""

        if not self._initialized:
            self.initialize()

        self.logger.info("Searching all embeddings for query.")

        try:
            if include_scores:
                documents_with_scores = self.vector_store_manager.similarity_search_with_score(
                    query=query,
                    k=max_results
                )
                return [
                    SearchResult.from_documents(doc, score)
                    for doc, score in documents_with_scores
                ]

            else:
                documents = self.vector_store_manager.similarity_search(
                    query=query,
                    k=max_results
                )
                return [
                    SearchResult.from_documents(doc) for doc in documents
                ]
        except Exception as e:
            self.logger.error(f"Error during search: {e}")
            return []

    def _build_dataset_id_filter(self, dataset_ids: List[str]) -> models.Filter:
        """Build a Qdrant filter for specific dataset IDs.
           Returns a Qdrant Filter object that matches any of the provided dataset IDs."""
        
        if len(dataset_ids) == 1:
            return models.Filter(
                must=[
                    models.FieldCondition(
                        key='metadata.dataset_id',
                        match=models.MatchValue(value=dataset_ids[0])
                    )
                ]
            )
        else:
            conditions=[]
            for dataset_id in dataset_ids:
                conditions.append(
                    models.FieldCondition(
                        key='metadata.dataset_id',
                        match=models.MatchValue(value=dataset_id)
                    )
                )
            return models.Filter(should=conditions)