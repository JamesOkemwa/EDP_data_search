from typing import List, Dict, Any
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
from services.retrieval_service import SearchResult
import logging


class ResponseGenerator:
    """Generates natural language responses using Langchain based on retrieved datasets."""

    def __init__(self):
        self.llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.3)
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
              Search Context: {metadata}
              
              Please provide a helpful response that addresses the user's query and recommends relevant datasets.""")
        ])

        # create the chain
        self.chain = self.prompt | self.llm | StrOutputParser()

    