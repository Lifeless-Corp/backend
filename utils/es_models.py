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



class PMCSearchResult(BaseModel):
    """Model representing a single PMC search result"""
    doi: Optional[str] = None
    pmcid: Optional[str] = None
    pmid: Optional[str] = None
    title: str
    abstract: Optional[str] = None
    authors: List[Author] = []
    journal: Optional[Journal] = None
    publication_date: Optional[str] = None
    article_type: Optional[str] = None
    keywords: List[str] = []
    score: Optional[float] = None
    highlights: Optional[Dict[str, List[str]]] = None

    class Config:
        extra = "ignore"


class PMCSearchResponse(BaseModel):
    results: List[PMCSearchResult]
    total: int
    time: float
    page: int
    size: int
    query: str


class PMCSearchFilters(BaseModel):
    article_type: Optional[str] = None
    journal: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    author: Optional[str] = None


class IndexStats(BaseModel):
    document_count: int
    index_size_bytes: int
    index_size_mb: float
