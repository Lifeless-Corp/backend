from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List,  Optional
import os
import time
from dotenv import load_dotenv

from utils.es_utils import (
    connect_elasticsearch, 
    search_pmc_documents, 
    get_pmc_document_by_id,
    get_index_stats,
    create_pmc_index
)
from utils.es_models import (
    PMCSearchResponse, 
    PMCSearchResult, 
    PMCDocument, 
    PMCSearchFilters,
    IndexStats
)

# Load environment variables
load_dotenv()

app = FastAPI(
    title="Organa Search API",
    description="Search and retrieve PMC (PubMed Central) articles",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Elasticsearch connection
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "pmc_articles")

es_client = connect_elasticsearch(ES_HOST, ES_PORT)


@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup"""
    if not es_client.ping():
        print("Warning: Could not connect to Elasticsearch")
    
    # Check if index exists, create if not
    if not es_client.indices.exists(index=ES_INDEX):
        print(f"Index {ES_INDEX} does not exist. Creating...")
        create_pmc_index(es_client, ES_INDEX)


@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "message": "Organa Search API", 
        "version": "2.0.0",
        "status": "healthy" if es_client.ping() else "elasticsearch_down"
    }


@app.get("/articles/search", response_model=PMCSearchResponse, tags=["Search"])
async def search_articles(
    query: str = Query(..., min_length=1, description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(10, ge=1, le=50, description="Number of results per page"),
    article_type: Optional[str] = Query(None, description="Filter by article type"),
    journal: Optional[str] = Query(None, description="Filter by journal"),
    date_from: Optional[str] = Query(None, description="Filter from date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Filter to date (YYYY-MM-DD)"),
):
    """
    Search PMC articles by query with optional filters.
    """
    try:
        start_time = time.time()
        from_idx = (page - 1) * size
        
        # Build filters
        filters = {}
        if article_type:
            filters['article_type'] = article_type
        if journal:
            filters['journal'] = journal
        if date_from:
            filters['date_from'] = date_from
        if date_to:
            filters['date_to'] = date_to
        
        # Search documents
        results, total = search_pmc_documents(
            es_client, 
            ES_INDEX, 
            query, 
            from_idx, 
            size, 
            filters if filters else None
        )
        
        search_time = time.time() - start_time
        
        # Convert to response model
        search_results = [PMCSearchResult(**result) for result in results]
        
        return PMCSearchResponse(
            results=search_results,
            total=total,
            time=round(search_time, 3),
            page=page,
            size=size,
            query=query
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")


@app.get("/articles/{article_id}", response_model=PMCDocument, tags=["Articles"])
async def get_article(article_id: str):
    """
    Retrieve a single PMC article by DOI, PMC ID, PMID.
    """
    try:
        document = get_pmc_document_by_id(es_client, ES_INDEX, article_id)
        
        if not document:
            raise HTTPException(status_code=404, detail="Article not found")
        
        return PMCDocument(**document)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving article: {str(e)}")


@app.get("/articles/{article_id}/similar", response_model=List[PMCSearchResult], tags=["Articles"])
async def get_similar_articles(
    article_id: str,
    size: int = Query(5, ge=1, le=20, description="Number of similar articles")
):
    """
    Find articles similar to the given article.
    """
    try:
        # Get the original article
        document = get_pmc_document_by_id(es_client, ES_INDEX, article_id)
        
        if not document:
            raise HTTPException(status_code=404, detail="Article not found")
        
        # Use title and keywords for similarity search
        search_terms = []
        if document.get('title'):
            search_terms.append(document['title'])
        if document.get('keywords'):
            search_terms.extend(document['keywords'])
        
        query = " ".join(search_terms[:100])  # Limit query length
        
        # Search for similar articles
        results, _ = search_pmc_documents(es_client, ES_INDEX, query, 0, size + 1)
        
        # Remove the original article from results
        similar_results = [r for r in results if r.get('pmcid') != article_id and r.get('pmid') != article_id][:size]
        
        return [PMCSearchResult(**result) for result in similar_results]
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error finding similar articles: {str(e)}")


@app.get("/stats", response_model=IndexStats, tags=["Statistics"])
async def get_statistics():
    """
    Get index statistics.
    """
    try:
        stats = get_index_stats(es_client, ES_INDEX)
        
        if "error" in stats:
            raise HTTPException(status_code=500, detail=stats["error"])
        
        return IndexStats(**stats)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting statistics: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)