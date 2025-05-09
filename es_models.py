from pydantic import BaseModel
from typing import List, Optional


class SearchResult(BaseModel):
    """Model representing a single search result compatible with frontend"""
    title: str
    url: str
    description: str
    favicon: Optional[str] = None


class SearchResponse(BaseModel):
    """Model representing the complete search response"""
    results: List[SearchResult]
    total: int
    time: float
    page: int
    limit: int
