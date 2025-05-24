from utils.es_utils import connect_elasticsearch, create_pmc_index, index_pmc_xml_directory
import os
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Elasticsearch connection settings
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "pmc_articles")

# Path to PMC XML files
XML_DIRECTORY = os.getenv("PMC_XML_DIRECTORY", "./corpus")


def main():
    """Main function to bulk load PMC XML data"""
    print("Starting PMC XML bulk loading process...")

    try:
        # Connect to Elasticsearch
        es_client = connect_elasticsearch(ES_HOST, ES_PORT)

        if not es_client.ping():
            print("Failed to connect to Elasticsearch. Exiting.")
            return

        # Create index with PMC mappings
        print("Creating PMC index...")
        create_pmc_index(es_client, ES_INDEX)

        # Check if XML directory exists
        if not os.path.exists(XML_DIRECTORY):
            print(f"XML directory not found: {XML_DIRECTORY}")
            print("Please ensure the PMC XML files are in the correct directory.")
            return

        # Index all XML files in the directory with smaller batch size
        print(f"Starting to index XML files from: {XML_DIRECTORY}")
        index_pmc_xml_directory(es_client, ES_INDEX,
                                XML_DIRECTORY, batch_size=25)

        # Get final stats
        from utils.es_utils import get_index_stats
        stats = get_index_stats(es_client, ES_INDEX)
        print(f"\nFinal Statistics:")
        print(
            f"Total documents indexed: {stats.get('document_count', 'Unknown')}")
        print(f"Index size: {stats.get('index_size_mb', 'Unknown')} MB")

        print("PMC XML bulk loading completed!")

    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
