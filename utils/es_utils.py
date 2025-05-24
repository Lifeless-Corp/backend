from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError
from typing import Dict, Any, List, Tuple, Optional
from utils.pmc_parser import PMCXMLParser
import glob
import os
import logging
from tqdm import tqdm  # Import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_elasticsearch(host: str = "localhost", port: str = "9200") -> Elasticsearch:
    es = Elasticsearch(f"http://{host}:{port}")
    if es.ping():
        print("Connected to Elasticsearch")
    else:
        print("Could not connect to Elasticsearch")
    return es


def create_pmc_index(es: Elasticsearch, index_name: str) -> bool:
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted existing index: {index_name}")

    mappings = {
        "mappings": {
            "properties": {
                "doi": {"type": "keyword"},
                "pmcid": {"type": "keyword"},
                "pmid": {"type": "keyword"},
                "title": {
                    "type": "text", "analyzer": "standard",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}
                },
                "abstract": {"type": "text", "analyzer": "standard"},
                "full_text": {"type": "text", "analyzer": "standard"},
                "authors": {
                    "type": "nested",
                    "properties": {
                        "full_name": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                        },
                        "orcid": {"type": "keyword"}
                    }
                },
                "journal": {
                    "properties": {
                        "title": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}
                        },
                        "issn": {"type": "keyword"}
                    }
                },
                "publication_date": {"type": "date", "format": "yyyy-MM-dd", "ignore_malformed": True},
                "article_type": {"type": "keyword"},
                "keywords": {"type": "keyword"},
                "processed_at": {"type": "date"}
            }
        },
        "settings": {
            "number_of_shards": 1, "number_of_replicas": 0,
            "index": {
                "refresh_interval": "30s",
                "max_result_window": 50000
            },
            "analysis": {
                "analyzer": {
                    "scientific_analyzer": {
                        "type": "custom", "tokenizer": "standard",
                        "filter": ["lowercase", "stop"]
                    }
                }
            }
        }
    }
    es.indices.create(index=index_name, body=mappings)
    print(
        f"Created index: {index_name} with refresh_interval set to -1 for bulk loading.")
    return True


def sanitize_document(doc: Dict) -> Optional[Dict]:
    try:
        if not any([doc.get('doi'), doc.get('pmcid'), doc.get('pmid')]) or not doc.get('title'):
            return None

        sanitized = {}
        sanitized['doi'] = str(doc.get('doi', '')).strip()
        sanitized['pmcid'] = str(doc.get('pmcid', '')).strip()
        sanitized['pmid'] = str(doc.get('pmid', '')).strip()
        sanitized['title'] = str(doc.get('title', '')).strip()[:500]

        abstract = doc.get('abstract', '')
        if abstract:
            sanitized['abstract'] = str(abstract).strip()[:5000]

        full_text = doc.get('full_text', '')
        if full_text:
            sanitized['full_text'] = str(full_text).strip()[:100000]

        authors = doc.get('authors', [])
        if isinstance(authors, list):
            clean_authors = []
            for author in authors[:20]:
                if isinstance(author, dict) and author.get('full_name'):
                    clean_author = {'full_name': str(
                        author['full_name']).strip()[:100]}
                    if author.get('orcid'):
                        clean_author['orcid'] = str(author['orcid']).strip()
                    clean_authors.append(clean_author)
            sanitized['authors'] = clean_authors

        journal = doc.get('journal', {})
        if isinstance(journal, dict):
            sanitized['journal'] = {
                'title': str(journal.get('title', 'Unknown Journal')).strip()[:200],
                'issn': str(journal.get('issn', '')).strip()
            }

        if doc.get('publication_date'):
            date_str = str(doc['publication_date']).strip()
            if len(date_str) == 10 and '-' in date_str:
                sanitized['publication_date'] = date_str

        sanitized['article_type'] = str(
            doc.get('article_type', 'research-article')).strip()

        keywords = doc.get('keywords', [])
        if isinstance(keywords, list):
            sanitized['keywords'] = [str(k).strip()
                                     for k in keywords if k][:20]

        sanitized['processed_at'] = doc.get('processed_at')
        return sanitized

    except Exception as e:
        logger.error(
            f"Error sanitizing document ({doc.get('doi', doc.get('pmcid', 'N/A'))}): {str(e)}")
        return None


def search_pmc_documents(
    es: Elasticsearch, index_name: str, query: str,
    from_idx: int = 0, size: int = 10, filters: Optional[Dict] = None
) -> Tuple[List[Dict[str, Any]], int]:

    search_query_body = {
        "query": {
            "bool": {
                "must": [{
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "title^4",
                            "abstract^3",
                            "keywords^3",
                            "full_text^1",
                            "authors.full_name^2"
                        ],
                        "type": "best_fields", "tie_breaker": 0.3
                    }
                }]
            }
        },
        "highlight": {
            "fields": {
                "title": {"number_of_fragments": 0},
                "abstract": {"fragment_size": 150, "number_of_fragments": 2},
                "full_text": {"fragment_size": 150, "number_of_fragments": 1}
            },
            "pre_tags": ["<mark>"], "post_tags": ["</mark>"]
        },
        "_source": {"excludes": []},
        "from": from_idx, "size": size
    }

    if filters:
        filter_clauses = []
        if filters.get('article_type'):
            filter_clauses.append(
                {"term": {"article_type": filters['article_type']}})
        if filters.get('journal'):
            filter_clauses.append(
                {"term": {"journal.title.keyword": filters['journal']}})
        if filters.get('author'):
            filter_clauses.append({
                "nested": {"path": "authors", "query": {"match": {"authors.full_name": filters['author']}}}
            })
        if filters.get('date_from') or filters.get('date_to'):
            date_range = {}
            if filters.get('date_from'):
                date_range['gte'] = filters['date_from']
            if filters.get('date_to'):
                date_range['lte'] = filters['date_to']
            filter_clauses.append({"range": {"publication_date": date_range}})
        if filter_clauses:
            search_query_body["query"]["bool"]["filter"] = filter_clauses

    try:
        response = es.search(index=index_name, body=search_query_body)
        hits = response["hits"]["hits"]
        total = response["hits"]["total"]["value"]
        results = []
        for hit in hits:
            result = hit["_source"]
            result["score"] = hit["_score"]
            if "highlight" in hit:
                result["_highlights"] = hit["highlight"]
            results.append(result)
        return results, total
    except Exception as e:
        logger.error(f"Search error: {str(e)}")
        return [], 0


def bulk_index_pmc_documents(es: Elasticsearch, index_name: str, documents: List[Dict], quiet: bool = False):
    actions = []
    skipped_count = 0
    for i, doc_content in enumerate(documents):
        sanitized_doc = sanitize_document(doc_content)
        if not sanitized_doc:
            skipped_count += 1
            continue
        doc_id = (sanitized_doc.get('doi') or sanitized_doc.get('pmcid') or
                  sanitized_doc.get('pmid') or f"doc_batch_{i}")
        actions.append({'_index': index_name, '_id': doc_id,
                       '_source': sanitized_doc})

    if not actions:
        if not quiet:
            print(
                f"⏭️ No valid documents to index in this batch (skipped {skipped_count})")
        return 0, 0, skipped_count  # Return counts

    success_count, error_count = 0, 0
    try:
        for success, info in helpers.streaming_bulk(
            es, actions, chunk_size=500,  # Ensure this is a good size
            max_retries=3, initial_backoff=2, max_backoff=600,
            raise_on_error=False, raise_on_exception=False,
            request_timeout=30
        ):
            if success:
                success_count += 1
            else:
                error_count += 1
                # Log individual document errors less verbosely, or collect them
                if isinstance(info, dict):
                    action_type = list(info.keys())[0]
                    error_detail = info[action_type]
                    # Limit logging for individual errors during bulk to avoid flooding
                    if error_count <= 10:  # Log first 10 errors in detail per batch
                        logger.error(
                            f"❌ Error indexing document {error_detail.get('_id', 'unknown')}: {error_detail.get('error', {}).get('reason', 'Unknown')}")
                    elif error_count == 11:
                        logger.error(
                            "More indexing errors in this batch, further detail suppressed...")
    except Exception as e:
        logger.error(f"❌ Bulk indexing stream failed: {str(e)}")
        # Return current counts as the stream broke
        return success_count, error_count + (len(actions) - success_count - error_count), skipped_count

    if not quiet or error_count > 0:  # Print if not quiet or if there were errors
        print(
            f"✅ Batch indexed: {success_count} documents, ❌ Errors: {error_count}, ⏭️ Skipped in sanitize: {skipped_count}")
    return success_count, error_count, skipped_count


def index_pmc_xml_directory(es: Elasticsearch, index_name: str, xml_directory: str, batch_size: int = 500):
    parser = PMCXMLParser()
    xml_files = glob.glob(os.path.join(xml_directory, "*.xml"))

    if not xml_files:
        print(f"No XML files found in {xml_directory}.")
        return

    print(
        f"📁 Found {len(xml_files)} XML files in {xml_directory}. Starting processing...")
    documents_batch = []
    total_docs_successfully_indexed = 0
    total_docs_with_errors = 0
    total_docs_skipped_sanitize = 0

    # Wrap the iterator with tqdm for a progress bar
    with tqdm(total=len(xml_files), desc="Processing XML files", unit="file") as pbar:
        for i, xml_file in enumerate(xml_files):
            try:
                doc_content = parser.parse_xml_file(xml_file)
                if doc_content:
                    documents_batch.append(doc_content)

                # Index in batches or if it's the last file
                if len(documents_batch) >= batch_size or (i == len(xml_files) - 1 and documents_batch):
                    if documents_batch:
                        s, e, sk = bulk_index_pmc_documents(
                            es, index_name, documents_batch, quiet=True)
                        total_docs_successfully_indexed += s
                        total_docs_with_errors += e
                        total_docs_skipped_sanitize += sk
                        documents_batch = []
            except Exception as e:
                logger.error(
                    f"Critical error processing file {xml_file}: {str(e)}")
            pbar.update(1)

    print(f"🎉 Completed XML processing and indexing.")
    print(
        f"   Total documents successfully indexed: {total_docs_successfully_indexed}")
    print(f"   Total documents with indexing errors: {total_docs_with_errors}")
    print(
        f"   Total documents skipped during sanitization: {total_docs_skipped_sanitize}")


def get_pmc_document_by_id(es: Elasticsearch, index_name: str, doc_id: str) -> Optional[Dict]:
    try:
        try:
            response = es.get(index=index_name, id=doc_id)
            return response["_source"]
        except NotFoundError:
            pass

        search_body = {
            "query": {"bool": {"should": [
                {"term": {"doi": doc_id}}, {"term": {"pmcid": doc_id}}, {
                    "term": {"pmid": doc_id}}
            ]}}, "size": 1
        }
        response = es.search(index=index_name, body=search_body)
        if response["hits"]["hits"]:
            return response["hits"]["hits"][0]["_source"]
        return None
    except Exception as e:
        logger.error(f"Error getting document {doc_id}: {str(e)}")
        return None


def get_index_stats(es: Elasticsearch, index_name: str) -> Dict:
    try:
        # Ensure index exists before getting stats to avoid error on empty/just created index
        if not es.indices.exists(index=index_name):
            logger.warning(
                f"Index {index_name} does not exist. Cannot get stats.")
            return {"error": f"Index {index_name} does not exist.", "document_count": 0, "index_size_bytes": 0, "index_size_mb": 0}

        stats = es.indices.stats(index=index_name, metric="store")
        count_response = es.count(index=index_name)
        doc_count = count_response["count"]

        # Handle potentially missing stats for a completely empty index after creation
        size_bytes = stats.get("_all", {}).get("primaries", {}).get(
            "store", {}).get("size_in_bytes", 0)

        return {
            "document_count": doc_count,
            "index_size_bytes": size_bytes,
            "index_size_mb": round(size_bytes / (1024 * 1024), 2)
        }
    except Exception as e:
        logger.error(f"Error getting index stats for {index_name}: {str(e)}")
        return {"error": str(e), "document_count": 0, "index_size_bytes": 0, "index_size_mb": 0}
