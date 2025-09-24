import os
from typing import List, Dict, Any, Optional
import logging
from parsers.query_parser import QueryParser, QueryIntent
from services.retrieval_service import DatasetRetrievalService, SearchResult
from geocoder.geocoding import GeocodingService, BoundingBox
from pg_database.postgis_db import PostGISService
from services.response_generator import ResponseGenerator


class RAGOrchestrator:
    """Main orchestrator that coordinates all existing services for the RAG pipeline"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # initialize existing services
        self.query_parser = QueryParser()
        self.geocoder = GeocodingService()
        self.retrieval_service = DatasetRetrievalService()
        self.response_generator = ResponseGenerator()
        self.postgis_service = None
        self.min_score = float(os.getenv("THRESHOLD_MIN_SCORE"))

    def initialize(self):
        """Initialize all services"""
        try:
            self.retrieval_service.initialize()
            self.postgis_service = PostGISService()
            self.postgis_service.connect()

            self.logger.info("RAG Orchestrator initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize the RAG orchestrator: {e}")
            raise

    def process_query(self, user_query: str, max_results: int=3) ->Dict[str, Any]:
        """
            Process user query through complete RAG pipeline
            Returns a dictionary with answer and the retrived datasets
        """
        try:
            # step 1 - Parse the query using the query parser
            self.logger.info(f"Processing query: {user_query}")
            parsed_query = self.query_parser.parse(user_query)

            # step 2 - Route the query based on whether a location is mentioned
            if parsed_query.has_location:
                result = self._process_location_based_query(parsed_query, user_query, max_results)
            else:
                result = self._process_semantic_only_query(parsed_query, user_query, max_results)
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error processing query: {e}")
            return {
                "answer": "I encountered an error processing your query. Please try again",
                "source_datasets": []
            }
        
    def _process_location_based_query(self, parsed_query: QueryIntent, original_query, max_results: int) -> Dict[str, Any]:
        """Process queries that mention specific locations"""
        self.logger.info("Processing location-based query")

        # geocode location
        location = parsed_query.locations[0]
        bounding_box = self.geocoder.get_bounding_box(location)

        if not bounding_box:
            self.logger.warning("No valid bounding box found")

            # generate response
            response = self.response_generator.generate_response(
                original_query=original_query,
                search_results=[],
            )
            return response
        
        # query postgis for datasets intersecting with the bounding box
        dataset_ids = self.postgis_service.find_dataset_ids_by_bbox(bounding_box)

        if len(dataset_ids) == 0:
            self.logger.info("No datasets intersect with the query bounding box")

            # generate response
            response = self.response_generator.generate_response(
                original_query=original_query,
                search_results=[],
            )
            return response
        
        # perform filtered semantic search
        search_query = " ".join(parsed_query.core_search_terms)
        search_results = self.retrieval_service.search_by_dataset_ids(
            query=search_query,
            dataset_ids=dataset_ids,
            max_results=max_results,
            min_score=self.min_score
        )

        # generate response
        response = self.response_generator.generate_response(
            original_query=original_query,
            search_results=search_results,
        )

        return response

    def _process_semantic_only_query(self, parsed_query: QueryIntent, original_query, max_results: int) -> Dict[str, Any]:
        """Process queries without specific location mentions"""
        self.logger.info("Processing semantic-only query")

        search_query = " ".join(parsed_query.core_search_terms) if parsed_query.core_search_terms else original_query
        search_results = self.retrieval_service.search_all_embeddings(
            query=search_query,
            max_results=max_results,
            min_score=self.min_score
        )

        # generate response
        response = self.response_generator.generate_response(
            original_query=original_query,
            search_results=search_results,
        )

        return response