from fastapi import FastAPI, HTTPException, Query
from elasticsearch import Elasticsearch
from typing import List, Dict, Any
import os
from dotenv import load_dotenv

app = FastAPI()

# Load environment variables
load_dotenv()

# Elasticsearch connection
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "pubmed_articles")

es_client = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")


@app.on_event("startup")
async def startup_event():
    if not es_client.indices.exists(index=ES_INDEX):
        es_client.indices.create(index=ES_INDEX, body={
            "mappings": {
                "properties": {
                    "pmid": {"type": "keyword"},
                    "title": {"type": "text"},
                    "abstract": {"type": "text"},
                    "mesh_terms": {"type": "keyword"},
                    "authors": {"type": "keyword"},
                    "journal": {"type": "text"},
                    "pub_year": {"type": "integer"},
                    "pub_types": {"type": "keyword"}
                }
            }
        })


@app.get("/articles/search")
async def search_articles(
    query: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=50)
):
    """
    Search articles by query.
    """
    from_idx = (page - 1) * size
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["title^3", "abstract", "mesh_terms", "authors", "journal"]
            }
        },
        "from": from_idx,
        "size": size
    }
    response = es_client.search(index=ES_INDEX, body=body)
    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]
    results = [hit["_source"] for hit in hits]
    return {"total": total, "page": page, "size": size, "results": results}


@app.get("/articles/{pmid}")
async def get_article(pmid: str):
    """
    Retrieve a single article by PMID.
    """
    try:
        response = es_client.get(index=ES_INDEX, id=pmid)
        return response["_source"]
    except Exception:
        raise HTTPException(status_code=404, detail="Article not found")
