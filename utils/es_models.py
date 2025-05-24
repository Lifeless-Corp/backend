from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import date


class Author(BaseModel):
    full_name: str
    orcid: Optional[str] = None


class Journal(BaseModel):
    title: str
    issn: Optional[str] = None


class PMCDocument(BaseModel):
    """Model representing a PMC document - focused on search essentials"""
    # Identifiers
    doi: str
    pmcid: Optional[str] = None
    pmid: Optional[str] = None
    
    # Core content
    title: str
    abstract: Optional[str] = None
    full_text: Optional[str] = None
    
    # Key metadata
    authors: List[Author] = []
    journal: Journal
    publication_date: Optional[str] = None
    article_type: Optional[str] = None
    keywords: List[str] = []
    
    # Search sections 
    sections: Dict[str, str] = {}  # introduction, methods, results, conclusion, etc.


class PMCSearchResult(BaseModel):
    """Model representing a single PMC search result"""
    # Essential identifiers
    doi: Optional[str] = None
    pmcid: Optional[str] = None
    pmid: Optional[str] = None
    
    # Core content for display
    title: str
    abstract: Optional[str] = None
    
    # Key metadata
    authors: List[Author] = []
    journal: Optional[Journal] = None  # Make optional
    publication_date: Optional[str] = None
    article_type: Optional[str] = None
    keywords: List[str] = []
    
    # Search relevance - these should be truly optional
    score: Optional[float] = None
    highlights: Optional[Dict[str, List[str]]] = None

    class Config:
        # Allow extra fields from Elasticsearch
        extra = "ignore"


class PMCSearchResponse(BaseModel):
    """Model representing the complete PMC search response"""
    results: List[PMCSearchResult]
    total: int
    time: float
    page: int
    size: int
    query: str


class PMCSearchFilters(BaseModel):
    """Model for PMC search filters"""
    article_type: Optional[str] = None
    journal: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    author: Optional[str] = None


class IndexStats(BaseModel):
    """Model for index statistics"""
    document_count: int
    index_size_bytes: int
    index_size_mb: float