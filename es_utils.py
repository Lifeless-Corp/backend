from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from typing import Dict, Any, List, Tuple, Optional
import json


def connect_elasticsearch(host: str = "localhost", port: str = "9200") -> Elasticsearch:
    """Connect to Elasticsearch instance"""
    es = Elasticsearch(f"http://{host}:{port}")
    if es.ping():
        print("Connected to Elasticsearch")
    else:
        print("Could not connect to Elasticsearch")
    return es


def create_index(es: Elasticsearch, index_name: str) -> bool:
    """Create an index with appropriate mappings for web page data"""
    if es.indices.exists(index=index_name):
        return False

    # Define mappings for web data
    mappings = {
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "url": {"type": "keyword"},
                "description": {"type": "text"},
                "content": {"type": "text"},
                "favicon": {"type": "keyword"},
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }

    es.indices.create(index=index_name, body=mappings)
    return True


def search_documents(
    es: Elasticsearch,
    index_name: str,
    query: str,
    from_idx: int = 0,
    size: int = 10
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Search for documents in the index
    Returns: (results, total_count)
    """
    search_query = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["title^3", "description^2", "content"],
                "type": "best_fields"
            }
        },
        "from": from_idx,
        "size": size
    }

    response = es.search(index=index_name, body=search_query)
    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]

    # Format results
    results = [hit["_source"] for hit in hits]
    return results, total


def index_document(es: Elasticsearch, index_name: str, document: Dict[str, Any]) -> str:
    """
    Index a document in Elasticsearch
    Returns: document ID
    """
    response = es.index(index=index_name, document=document)
    return response["_id"]


def index_sample_data(es: Elasticsearch, index_name: str) -> None:
    """Create index and add sample web data for testing"""
    # Create index first
    create_index(es, index_name)

    # Sample data that matches the frontend mock data
    sample_data = [
        {
            "title": "Nuxt.js - Kerangka Kerja Vue.js Intuitif",
            "url": "https://nuxt.com",
            "description": "Nuxt adalah kerangka kerja Vue.js yang intuitif. Ini memudahkan pembuatan aplikasi Vue.js dengan server-side rendering, static site generation, dan banyak fitur lainnya.",
            "content": "Nuxt adalah kerangka kerja Vue.js yang intuitif untuk membuat aplikasi web modern. Nuxt.js menyediakan arsitektur berbasis komponen yang kuat dengan abstraksi yang bagus untuk membangun aplikasi web dan situs statis. Nuxt 3 adalah kerangka kerja Vue 3 yang sepenuhnya ditulis dalam TypeScript, memanfaatkan Vite dan Nitro untuk kinerja yang luar biasa.",
            "favicon": "https://nuxt.com/icon.png"
        },
        {
            "title": "Vue.js - Framework JavaScript Progresif",
            "url": "https://vuejs.org",
            "description": "Vue.js adalah framework JavaScript progresif untuk membangun antarmuka pengguna. Tidak seperti framework monolitik lainnya, Vue dirancang dari awal untuk dapat diadopsi secara bertahap.",
            "content": "Vue (diucapkan /vjuː/, seperti view) adalah kerangka kerja JavaScript progresif untuk membangun antarmuka pengguna. Tidak seperti kerangka kerja monolitik lainnya, Vue dirancang dari awal untuk dapat diadopsi secara bertahap. Pustaka inti berfokus hanya pada lapisan tampilan, dan mudah untuk diintegrasikan dengan pustaka lain atau proyek yang sudah ada.",
            "favicon": "https://vuejs.org/logo.png"
        },
        {
            "title": "Tailwind CSS - Framework CSS Utility-First",
            "url": "https://tailwindcss.com",
            "description": "Tailwind CSS adalah framework CSS utility-first untuk membangun desain kustom dengan cepat tanpa meninggalkan HTML Anda.",
            "content": "Tailwind CSS adalah kerangka kerja CSS utility-first yang dikemas dengan kelas-kelas seperti flex, pt-4, text-center dan rotate-90 yang dapat disusun untuk membangun desain apa pun, langsung di markup Anda. Tailwind CSS memungkinkan pengembang untuk membangun dengan cepat, menyediakan kelas-kelas utilitas tingkat rendah yang dapat Anda gunakan untuk membangun desain kustom tanpa meninggalkan HTML Anda.",
            "favicon": "https://tailwindcss.com/favicon.ico"
        },
        {
            "title": "JavaScript - MDN Web Docs",
            "url": "https://developer.mozilla.org/en-US/docs/Web/JavaScript",
            "description": "JavaScript (JS) adalah bahasa pemrograman yang ringan, ditafsirkan, atau just-in-time dikompilasi dengan fungsi kelas satu.",
            "content": "JavaScript (JS) adalah bahasa pemrograman yang ringan, ditafsirkan, atau just-in-time dikompilasi dengan fungsi kelas satu. Sementara paling dikenal sebagai bahasa skrip untuk halaman Web, banyak lingkungan non-browser juga menggunakannya, seperti Node.js, Apache CouchDB dan Adobe Acrobat. JavaScript adalah bahasa berbasis prototipe, multi-paradigma, single-thread, dinamis, mendukung gaya pemrograman berorientasi objek, imperatif, dan deklaratif (misalnya pemrograman fungsional).",
            "favicon": "https://developer.mozilla.org/favicon.ico"
        },
        {
            "title": "TypeScript: JavaScript dengan Sintaks untuk Tipe",
            "url": "https://www.typescriptlang.org",
            "description": "TypeScript adalah JavaScript dengan sintaks untuk tipe. TypeScript adalah bahasa pemrograman open-source yang dibangun di atas JavaScript.",
            "content": "TypeScript adalah bahasa pemrograman yang dikembangkan dan dikelola oleh Microsoft. Ini adalah superset sintaksis ketat dari JavaScript dan menambahkan pengetikan statis opsional ke bahasa. TypeScript dirancang untuk pengembangan aplikasi besar dan transkompilasi ke JavaScript. Karena TypeScript adalah superset dari JavaScript, program JavaScript yang ada juga merupakan program TypeScript yang valid.",
            "favicon": "https://www.typescriptlang.org/favicon.ico"
        },
        {
            "title": "GitHub: Where the world builds software",
            "url": "https://github.com",
            "description": "GitHub adalah platform pengembangan perangkat lunak yang memungkinkan Anda menyimpan, melacak, dan berkolaborasi pada proyek perangkat lunak.",
            "content": "GitHub adalah layanan hosting untuk pengembangan perangkat lunak dan kontrol versi menggunakan Git. Ini menyediakan kontrol versi terdistribusi dan fungsi manajemen kode sumber dari Git, ditambah fitur-fiturnya sendiri. Ini memberikan kontrol akses dan beberapa fitur kolaborasi seperti pelacakan bug, permintaan fitur, manajemen tugas, dan wiki untuk setiap proyek.",
            "favicon": "https://github.com/favicon.ico"
        },
        {
            "title": "Stack Overflow - Where Developers Learn, Share, & Build",
            "url": "https://stackoverflow.com",
            "description": "Stack Overflow adalah komunitas pengembang terbesar dan paling tepercaya untuk berbagi pengetahuan pemrograman, membangun karier mereka, dan memecahkan masalah pemrograman mereka.",
            "content": "Stack Overflow adalah situs tanya jawab untuk programmer profesional dan antusias. Ini adalah platform bagi pengguna untuk mengajukan dan menjawab pertanyaan, dan, melalui keanggotaan dan partisipasi aktif, untuk memilih pertanyaan dan jawaban naik atau turun dan mengedit pertanyaan dan jawaban.",
            "favicon": "https://stackoverflow.com/favicon.ico"
        }
    ]

    # Index each document
    for doc in sample_data:
        index_document(es, index_name, doc)

    # Force refresh to make documents available for search immediately
    es.indices.refresh(index=index_name)
    print(f"Indexed {len(sample_data)} sample documents")


def index_jsonl_file(es_client, index_name: str, file_path: str):
    """
    Index articles from a JSONL file into Elasticsearch.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:

            article = json.loads(line)
            print(f"Indexing article with PMID: {article['pmid']}")
            es_client.index(index=index_name,
                            id=article["pmid"], document=article)
