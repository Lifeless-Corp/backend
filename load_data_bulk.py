from elasticsearch import Elasticsearch, helpers
import json

# Sesuaikan jika pakai port atau auth
es = Elasticsearch("http://localhost:9200")
# es = Elasticsearch("http://34.59.47.86:9200")

index_name = "pubmed_articles"

# Pastikan indeks sudah ada atau buat dulu:
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)


def load_jsonl(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            yield json.loads(line)


def generate_bulk_actions(file_path):
    for doc in load_jsonl(file_path):
        yield {
            "_index": index_name,
            "_source": doc
        }


# Jalankan bulk indexing
helpers.bulk(es, generate_bulk_actions("pubmed_articles.jsonl"))
