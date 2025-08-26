from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List
from contextlib import asynccontextmanager
import logging
from services.orchestrator import RAGOrchestrator

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryRequest(BaseModel):
    query: str
    max_results: int = 5

class QueryResponse(BaseModel):
    answer: str
    source_datasets: List[Dict[str, Any]]


rag_orchestrator = RAGOrchestrator()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # initialize the services once on application startup
    try:
        rag_orchestrator.initialize()
        logger.info("RAG services initialized successfully")
        yield
    except Exception as e:
        logger.error(f"Failed to initialize RAG services: {e}")
    finally:
        pass

app = FastAPI(
        title='Spatial Data Search',
        description='Natural language search for spatial datasets using RAG',
        lifespan=lifespan
    )

@app.post("/search", response_model=QueryResponse)
def search_datasets(request: QueryRequest):
    """Search for datasets using natural language query"""
    try:
        result = rag_orchestrator.process_query(
            user_query=request.query,
            max_results=request.max_results
        )
        return QueryResponse(
            answer=result.get("answer", ''),
            source_datasets=result.get("source_datasets", [])
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {e}")