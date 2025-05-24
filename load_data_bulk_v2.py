from elasticsearch import Elasticsearch, helpers
import json
import logging
import os
import glob
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Elasticsearch connection
ES_HOST = "http://localhost:9200"
# ES_HOST = "http://34.59.47.86:9200"
index_name = "pmc_articles"

# Files location
settings_file = "index/pmc_articles_settings.json"
mapping_file = "index/pmc_articles_mapping.json"
# Instead of a single data file, we'll use a pattern to match all split parts
data_files_pattern = "index/split_data/pmc_articles_data.jsonl.part-*"


def connect_elasticsearch():
    """Connect to Elasticsearch"""
    try:
        es_client = Elasticsearch(ES_HOST)
        if es_client.ping():
            logger.info("Connected to Elasticsearch")
            return es_client
        else:
            logger.error("Could not connect to Elasticsearch")
            return None
    except Exception as e:
        logger.error(f"Elasticsearch connection error: {str(e)}")
        return None


def load_jsonl(file_path):
    """Load JSONL file line by line"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    yield json.loads(line.strip())
                except json.JSONDecodeError:
                    logger.warning(f"Skipping invalid JSON line in {file_path}")
                    continue
    except UnicodeDecodeError:
        # Fall back to Latin-1 encoding if UTF-8 fails
        logger.warning(f"Falling back to Latin-1 encoding for {file_path}")
        with open(file_path, 'r', encoding='latin-1') as f:
            for line in f:
                try:
                    yield json.loads(line.strip())
                except json.JSONDecodeError:
                    logger.warning(f"Skipping invalid JSON line in {file_path}")
                    continue


def restore_settings_and_mapping(es_client):
    """Restore index settings and mapping from elasticdump export files"""
    try:
        # Check if index exists and delete if needed
        if es_client.indices.exists(index=index_name):
            logger.info(f"Deleting existing index: {index_name}")
            es_client.indices.delete(index=index_name)
        
        create_body = {}
        
        # Load mapping
        if os.path.exists(mapping_file):
            logger.info(f"Loading mapping from {mapping_file}")
            
            try:
                # Read the mapping file
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    mapping_data = json.load(f)
                
                # Extract mappings - handle the nested structure
                if index_name in mapping_data and 'mappings' in mapping_data[index_name]:
                    # Structure: {"pmc_articles": {"mappings": {...}}}
                    create_body['mappings'] = mapping_data[index_name]['mappings']
                elif 'mappings' in mapping_data:
                    # Structure: {"mappings": {...}}
                    create_body['mappings'] = mapping_data['mappings']
                else:
                    # Assume the whole object is mappings
                    create_body['mappings'] = mapping_data
                    
            except Exception as e:
                logger.error(f"Error parsing mapping file: {e}")
        else:
            logger.warning(f"Mapping file {mapping_file} not found. Using default mapping.")
        
        # Load settings
        if os.path.exists(settings_file):
            logger.info(f"Loading settings from {settings_file}")
            
            try:
                # Read the settings file
                with open(settings_file, 'r', encoding='utf-8') as f:
                    # Handle double encoding if present (JSON string within JSON file)
                    file_content = f.read()
                    if file_content.startswith('"') and file_content.endswith('"'):
                        # This is a JSON string inside a JSON file
                        settings_string = json.loads(file_content)
                        settings_data = json.loads(settings_string)
                    else:
                        # Normal JSON object
                        settings_data = json.loads(file_content)
                
                # Extract settings - handle the nested structure
                if index_name in settings_data and 'settings' in settings_data[index_name]:
                    # Structure: {"pmc_articles": {"settings": {...}}}
                    settings = settings_data[index_name]['settings']
                elif 'settings' in settings_data:
                    # Structure: {"settings": {...}}
                    settings = settings_data['settings']
                else:
                    # Assume the whole object is settings
                    settings = settings_data
                
                # Clean settings
                if 'index' in settings:
                    index_settings = settings['index']
                    for key in ['creation_date', 'uuid', 'version', 'provided_name']:
                        if key in index_settings:
                            del index_settings[key]
                
                create_body['settings'] = settings
                
            except Exception as e:
                logger.error(f"Error parsing settings file: {e}")
        else:
            logger.warning(f"Settings file {settings_file} not found. Using default settings.")
        
        # Debug the create body
        logger.info(f"Create body: {json.dumps(create_body)}")
        
        # Create index with settings and mappings
        logger.info(f"Creating index: {index_name}")
        es_client.indices.create(index=index_name, body=create_body)
        logger.info(f"Index {index_name} created successfully with custom settings and mapping")
        
        return True
        
    except Exception as e:
        logger.error(f"Error restoring settings and mapping: {str(e)}")
        logger.error(f"Exception type: {type(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def generate_bulk_actions_from_file(file_path):
    """Generate bulk actions from a single JSONL file"""
    for doc in load_jsonl(file_path):
        # Handle elasticdump format which might include _id and _source
        if '_source' in doc:
            action = {
                "_index": index_name,
                "_source": doc['_source']
            }
            # Preserve original document ID if present
            if '_id' in doc:
                action["_id"] = doc['_id']
        else:
            # If elasticdump directly exported source objects
            action = {
                "_index": index_name,
                "_source": doc
            }

        yield action


def count_lines_in_file(file_path):
    """Count lines in a file with appropriate encoding"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)
    except UnicodeDecodeError:
        # Fall back to Latin-1 encoding if UTF-8 fails
        logger.warning(f"Falling back to Latin-1 encoding for counting lines in {file_path}")
        with open(file_path, 'r', encoding='latin-1') as f:
            return sum(1 for _ in f)


def restore_data_from_split_files(es_client):
    """Restore data from split files"""
    try:
        # Get all split files that match the pattern
        data_files = sorted(glob.glob(data_files_pattern))
        
        if not data_files:
            logger.error(f"No data files found matching pattern: {data_files_pattern}")
            return False

        logger.info(f"Found {len(data_files)} data file parts")

        # Count total lines across all files for progress reporting
        total_lines = 0
        for file_path in data_files:
            try:
                file_lines = count_lines_in_file(file_path)
                total_lines += file_lines
                logger.info(f"Counted {file_lines} lines in {file_path}")
            except Exception as e:
                logger.error(f"Error counting lines in {file_path}: {str(e)}")
                return False
            
        logger.info(f"Total documents to import: {total_lines}")

        # Process each file in order
        batch_size = 500
        actions = []
        indexed_count = 0
        current_file_count = 0

        for file_num, file_path in enumerate(data_files, 1):
            logger.info(f"Processing file {file_num}/{len(data_files)}: {file_path}")
            
            try:
                # Process documents from current file
                doc_count = 0
                for action in generate_bulk_actions_from_file(file_path):
                    actions.append(action)
                    doc_count += 1
                    current_file_count += 1

                    if len(actions) >= batch_size:
                        success, failed = helpers.bulk(
                            es_client,
                            actions,
                            stats_only=True
                        )
                        indexed_count += success
                        logger.info(
                            f"Indexed {indexed_count}/{total_lines} documents ({indexed_count/total_lines*100:.1f}%)")
                        actions = []

                # Log progress after each file
                logger.info(f"Completed file {file_num}/{len(data_files)}, processed {doc_count} documents")
                
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                # Continue with next file
                continue

        # Index any remaining actions
        if actions:
            try:
                success, failed = helpers.bulk(
                    es_client,
                    actions,
                    stats_only=True
                )
                indexed_count += success
            except Exception as e:
                logger.error(f"Error in final bulk indexing: {str(e)}")

        logger.info(
            f"Data import completed. Total documents indexed: {indexed_count}")
        return True

    except Exception as e:
        logger.error(f"Error restoring data: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def main():
    """Main function to restore Elasticsearch index from split files"""
    start_time = datetime.now()
    logger.info(
        f"Starting Elasticsearch index restoration from split files")

    # Connect to Elasticsearch
    es_client = connect_elasticsearch()
    if not es_client:
        logger.error("Failed to connect to Elasticsearch. Exiting.")
        return

    # Restore settings and mapping
    if not restore_settings_and_mapping(es_client):
        logger.error("Failed to restore settings and mapping. Exiting.")
        return

    # Restore data from split files
    if not restore_data_from_split_files(es_client):
        logger.error("Failed to restore data. Exiting.")
        return

    # Get index stats
    try:
        stats = es_client.indices.stats(index=index_name)
        doc_count = stats['indices'][index_name]['total']['docs']['count']
        size_in_bytes = stats['indices'][index_name]['total']['store']['size_in_bytes']
        size_in_mb = size_in_bytes / (1024 * 1024)

        logger.info(f"Index Stats:")
        logger.info(f"Documents count: {doc_count}")
        logger.info(f"Index size: {size_in_mb:.2f} MB")
    except Exception as e:
        logger.error(f"Error getting index stats: {str(e)}")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Restoration completed in {duration:.2f} seconds")


if __name__ == "__main__":
    main()