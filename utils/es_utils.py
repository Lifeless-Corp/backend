from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError
from typing import Dict, Any, List, Tuple, Optional
from utils.pmc_parser import PMCXMLParser
import glob
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_elasticsearch(host: str = "localhost", port: str = "9200") -> Elasticsearch:
    """Connect to Elasticsearch instance"""
    es = Elasticsearch(f"http://{host}:{port}")
    if es.ping():
        print("Connected to Elasticsearch")
    else:
        print("Could not connect to Elasticsearch")
    return es


def create_pmc_index(es: Elasticsearch, index_name: str) -> bool:
    """Create an optimized index for PMC search"""
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted existing index: {index_name}")

    mappings = {
        "mappings": {
            "properties": {
                # Identifiers
                "doi": {"type": "keyword"},
                "pmcid": {"type": "keyword"},
                "pmid": {"type": "keyword"},
                
                # Core searchable content
                "title": {
                    "type": "text",
                    "analyzer": "standard",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 512}
                    }
                },
                "abstract": {
                    "type": "text",
                    "analyzer": "standard"
                },
                "full_text": {
                    "type": "text",
                    "analyzer": "standard"
                },
                
                # Metadata
                "authors": {
                    "type": "nested",
                    "properties": {
                        "full_name": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256}
                            }
                        },
                        "orcid": {"type": "keyword"}
                    }
                },
                "journal": {
                    "properties": {
                        "title": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 512}
                            }
                        },
                        "issn": {"type": "keyword"}
                    }
                },
                "publication_date": {
                    "type": "date", 
                    "format": "yyyy-MM-dd", 
                    "ignore_malformed": True
                },
                "article_type": {"type": "keyword"},
                "keywords": {"type": "keyword"},
                
                # Sections (dynamic mapping for flexibility)
                "sections": {
                    "type": "object",
                    "dynamic": True
                },
                
                # Processing metadata
                "processed_at": {"type": "date"}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "index": {
                "max_result_window": 50000
            },
            "analysis": {
                "analyzer": {
                    "scientific_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "stop"
                        ]
                    }
                }
            }
        }
    }

    es.indices.create(index=index_name, body=mappings)
    print(f"Created index: {index_name}")
    return True


def sanitize_document(doc: Dict) -> Optional[Dict]:
    """Clean and validate document for indexing"""
    try:
        # Must have at least one identifier
        if not any([doc.get('doi'), doc.get('pmcid'), doc.get('pmid')]):
            return None
        
        # Must have title
        if not doc.get('title'):
            return None
        
        sanitized = {}
        
        # Identifiers
        sanitized['doi'] = str(doc.get('doi', '')).strip()
        sanitized['pmcid'] = str(doc.get('pmcid', '')).strip() 
        sanitized['pmid'] = str(doc.get('pmid', '')).strip()
        
        # Core content
        sanitized['title'] = str(doc.get('title', '')).strip()[:500]  # Limit title length
        
        abstract = doc.get('abstract', '')
        if abstract:
            sanitized['abstract'] = str(abstract).strip()[:5000]  # Limit abstract
        
        full_text = doc.get('full_text', '')
        if full_text:
            sanitized['full_text'] = str(full_text).strip()[:100000]  # Limit full text
        
        # Authors
        authors = doc.get('authors', [])
        if isinstance(authors, list):
            clean_authors = []
            for author in authors[:20]:  # Limit to 20 authors
                if isinstance(author, dict) and author.get('full_name'):
                    clean_author = {
                        'full_name': str(author['full_name']).strip()[:100]
                    }
                    if author.get('orcid'):
                        clean_author['orcid'] = str(author['orcid']).strip()
                    clean_authors.append(clean_author)
            sanitized['authors'] = clean_authors
        
        # Journal
        journal = doc.get('journal', {})
        if isinstance(journal, dict):
            sanitized['journal'] = {
                'title': str(journal.get('title', 'Unknown Journal')).strip()[:200],
                'issn': str(journal.get('issn', '')).strip()
            }
        
        # Dates
        if doc.get('publication_date'):
            date_str = str(doc['publication_date']).strip()
            if len(date_str) == 10 and '-' in date_str:  # Basic YYYY-MM-DD check
                sanitized['publication_date'] = date_str
        
        # Article type
        sanitized['article_type'] = str(doc.get('article_type', 'research-article')).strip()
        
        # Keywords
        keywords = doc.get('keywords', [])
        if isinstance(keywords, list):
            clean_keywords = [str(k).strip() for k in keywords if k][:20]  # Limit keywords
            sanitized['keywords'] = clean_keywords
        
        # Sections
        sections = doc.get('sections', {})
        if isinstance(sections, dict):
            clean_sections = {}
            for key, value in sections.items():
                if key and value:
                    clean_key = str(key).strip()[:50]
                    clean_value = str(value).strip()[:10000]  # Limit section content
                    if clean_key and clean_value:
                        clean_sections[clean_key] = clean_value
            sanitized['sections'] = clean_sections
        
        # Processing metadata
        sanitized['processed_at'] = doc.get('processed_at')
        
        return sanitized
        
    except Exception as e:
        logger.error(f"Error sanitizing document: {str(e)}")
        return None


def search_pmc_documents(
    es: Elasticsearch,
    index_name: str,
    query: str,
    from_idx: int = 0,
    size: int = 10,
    filters: Optional[Dict] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """Optimized search for PMC documents"""
    
    search_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": [
                                "title^4",           # Title most important
                                "abstract^3",        # Abstract very important
                                "keywords^3",        # Keywords very important
                                "sections.*^2",      # Section content important
                                "full_text^1",       # Full text baseline
                                "authors.full_name^2" # Author names important
                            ],
                            "type": "best_fields",
                            "tie_breaker": 0.3
                        }
                    }
                ]
            }
        },
        "highlight": {
            "fields": {
                "title": {"number_of_fragments": 0},
                "abstract": {"fragment_size": 150, "number_of_fragments": 2},
                "sections.*": {"fragment_size": 150, "number_of_fragments": 1}
            },
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"]
        },
        "_source": {
            "excludes": ["full_text"]  # Don't return full text in results
        },
        "from": from_idx,
        "size": size
    }

    # Add filters
    if filters:
        filter_clauses = []
        
        if filters.get('article_type'):
            filter_clauses.append({"term": {"article_type": filters['article_type']}})
        
        if filters.get('journal'):
            filter_clauses.append({"term": {"journal.title.keyword": filters['journal']}})
        
        if filters.get('author'):
            filter_clauses.append({
                "nested": {
                    "path": "authors",
                    "query": {
                        "match": {"authors.full_name": filters['author']}
                    }
                }
            })
        
        if filters.get('date_from') or filters.get('date_to'):
            date_range = {}
            if filters.get('date_from'):
                date_range['gte'] = filters['date_from']
            if filters.get('date_to'):
                date_range['lte'] = filters['date_to']
            filter_clauses.append({"range": {"publication_date": date_range}})
        
        if filter_clauses:
            search_query["query"]["bool"]["filter"] = filter_clauses

    try:
        response = es.search(index=index_name, body=search_query)
        hits = response["hits"]["hits"]
        total = response["hits"]["total"]["value"]

        results = []
        for hit in hits:
            result = hit["_source"]
            result["_score"] = hit["_score"]
            if "highlight" in hit:
                result["_highlights"] = hit["highlight"]
            results.append(result)
        
        return results, total
        
    except Exception as e:
        logger.error(f"Search error: {str(e)}")
        return [], 0


def bulk_index_pmc_documents(es: Elasticsearch, index_name: str, documents: List[Dict]):    
    actions = []
    skipped = 0
    
    for i, doc in enumerate(documents):
        sanitized_doc = sanitize_document(doc)
        
        if not sanitized_doc:
            skipped += 1
            continue
        
        # Use DOI as primary ID, fallback to PMC ID or PMID
        doc_id = (sanitized_doc.get('doi') or 
                 sanitized_doc.get('pmcid') or 
                 sanitized_doc.get('pmid') or 
                 f"doc_{i}")
        
        actions.append({
            '_index': index_name,
            '_id': doc_id,
            '_source': sanitized_doc
        })
    
    if not actions:
        print(f"❌ No valid documents to index (skipped {skipped})")
        return
    
    success_count = 0
    error_count = 0
    
    try:
        for success, info in helpers.streaming_bulk(
            es,
            actions,
            chunk_size=25,
            max_retries=3,
            initial_backoff=2,
            max_backoff=600,
            raise_on_error=False,
            raise_on_exception=False
        ):
            if success:
                success_count += 1
            else:
                error_count += 1
                # Log error details
                if isinstance(info, dict):
                    action_type = list(info.keys())[0]
                    error_detail = info[action_type]
                    print(f"❌ Error indexing document {error_detail.get('_id', 'unknown')}")
                    if 'error' in error_detail:
                        print(f"   Reason: {error_detail['error'].get('reason', 'Unknown')}")
    
    except Exception as e:
        print(f"❌ Bulk indexing failed: {str(e)}")
        return
    
    print(f"✅ Indexed {success_count} documents, ❌ {error_count} errors, ⏭️ {skipped} skipped")
    
    # Refresh index
    try:
        es.indices.refresh(index=index_name)
    except Exception as e:
        logger.warning(f"Failed to refresh index: {str(e)}")


def index_pmc_xml_directory(es: Elasticsearch, index_name: str, xml_directory: str, batch_size: int = 20):
    """Index PMC XML files optimized for search"""
    parser = PMCXMLParser()
    
    xml_files = glob.glob(os.path.join(xml_directory, "*.xml"))
    print(f"📁 Found {len(xml_files)} XML files")
    
    documents = []
    total_processed = 0
    
    for i, xml_file in enumerate(xml_files):
        try:
            print(f"📄 Processing {i+1}/{len(xml_files)}: {os.path.basename(xml_file)}")
            
            doc = parser.parse_xml_file(xml_file)
            
            if doc:
                documents.append(doc)
                total_processed += 1
            
            # Process in batches
            if len(documents) >= batch_size or i == len(xml_files) - 1:
                if documents:
                    print(f"📤 Indexing batch of {len(documents)} documents...")
                    bulk_index_pmc_documents(es, index_name, documents)
                    documents = []
                    
                    # Brief pause
                    import time
                    time.sleep(0.1)
        
        except Exception as e:
            logger.error(f"Error processing {xml_file}: {str(e)}")
            continue
    
    print(f"🎉 Completed! Processed {total_processed} documents")


def get_pmc_document_by_id(es: Elasticsearch, index_name: str, doc_id: str) -> Optional[Dict]:
    """Get document by any ID (DOI, PMC ID, or PMID)"""
    try:
        # Try direct lookup first
        try:
            response = es.get(index=index_name, id=doc_id)
            return response["_source"]
        except NotFoundError:
            pass
        
        # Search by different ID fields
        search_query = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"doi": doc_id}},
                        {"term": {"pmcid": doc_id}},
                        {"term": {"pmid": doc_id}}
                    ]
                }
            },
            "size": 1
        }
        
        response = es.search(index=index_name, body=search_query)
        hits = response["hits"]["hits"]
        
        if hits:
            return hits[0]["_source"]
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting document {doc_id}: {str(e)}")
        return None


def get_index_stats(es: Elasticsearch, index_name: str) -> Dict:
    """Get index statistics"""
    try:
        stats = es.indices.stats(index=index_name)
        count_response = es.count(index=index_name)
        
        return {
            "document_count": count_response["count"],
            "index_size_bytes": stats["indices"][index_name]["total"]["store"]["size_in_bytes"],
            "index_size_mb": round(stats["indices"][index_name]["total"]["store"]["size_in_bytes"] / (1024 * 1024), 2)
        }
    except Exception as e:
        return {"error": str(e)}