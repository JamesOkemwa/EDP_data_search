from typing import List, Dict, Any
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
from services.retrieval_service import SearchResult
import logging


class ResponseGenerator:
    """Generates natural language responses using Langchain based on retrieved datasets."""

    def __init__(self):
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3)
        self.logger = logging.getLogger(__name__)

        # response generation prompt
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a helpful assistant for a spatial data search system.
             Based on the user's query and the retrieved datasets, provide a consise, informative response.
             
             Guidelines:
             - Directly address the user's query.
             - Recommend the most relevant datasets.
             - Mention location context if applicable.
             - Be helpful and specific."""),

             ("human", """User Query: {original_query}
              Retrieved Datasets: {datasets_info}
              
              Please provide a helpful response that addresses the user's query and recommends relevant datasets.""")
        ])

        # create the chain
        self.chain = self.prompt | self.llm | StrOutputParser()

    def generate_response(self, original_query: str, search_results: List[SearchResult]) -> Dict[str, Any]:
        """
        Generate a natural language response based on search results.

        Args:
            original query: User's original query
            search_results: List of SearchResult objects from vector search

        Returns:
            Dictionary with generated response and formatted datasets.
        """

        try:
            # format datasets for the prompt
            datasets_info = self._format_datasets_for_prompt(search_results)

            # generate response using Langchain
            response_text = self.chain.invoke({
                "original_query": original_query,
                "datasets_info": datasets_info,
            })

            # format source datasets for the API response
            formatted_datasets = self._format_datasets_for_response(search_results)

            return {
                "answer": response_text,
                "source_datasets": formatted_datasets
            }
        
        except Exception as e:
            self.logger.error(f"Error generating response: {e}")
            return {
                "answer": f"I found {len(search_results)} datsasets related to your query, but could not generate a detailed response.",
                "source_datasets": self._format_datasets_for_response(search_results)
            }
        
    def _format_datasets_for_prompt(self, search_results: List[SearchResult]) -> str:
        """Format datasets for inclusion in the LLM prompt"""
        if not search_results:
            return "No datasets found"
        
        formatted = []
        for i, result in enumerate(search_results, 1):
            score_info = f" (Relevance: {result.score})" if result.score else ""
            formatted.append(f"{i}. Dataset ID: {result.dataset_id} {score_info} \n Content: {result.content}")
        
        return "\n\n".join(formatted)
    
    def _format_datasets_for_response(self, search_results: List[SearchResult]) -> List[Dict[str, Any]]:
        """Format datasets for API response."""
        formatted = []
        for result in search_results:
            dataset_info = {
                "dataset_id": result.dataset_id,
                "relevance_score": result.score,
                "metadata": result.metadata
            }
            formatted.append(dataset_info)

        return formatted