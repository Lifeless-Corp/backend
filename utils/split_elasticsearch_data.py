import os
import argparse
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def split_file(input_file, output_dir, chunk_size):
    """
    Split a large file into smaller chunks

    Args:
        input_file: Path to the file to be split
        output_dir: Directory where chunks will be saved
        chunk_size: Size of each chunk in megabytes
    """
    # Convert MB to bytes
    chunk_bytes = chunk_size * 1024 * 1024

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get base filename for output
    base_name = os.path.basename(input_file)

    # Get total file size for progress tracking
    total_size = os.path.getsize(input_file)
    logger.info(
        f"Splitting file: {input_file} ({total_size / (1024 * 1024):.2f} MB)")
    logger.info(f"Chunk size: {chunk_size} MB")

    # Calculate expected number of chunks
    expected_chunks = (total_size + chunk_bytes - 1) // chunk_bytes
    logger.info(f"Expected number of chunks: {expected_chunks}")

    # Open the input file
    with open(input_file, 'rb') as f:
        chunk_num = 0
        bytes_processed = 0

        while True:
            # Read chunk
            chunk = f.read(chunk_bytes)

            # Break if we've reached end of file
            if not chunk:
                break

            chunk_num += 1
            chunk_file = os.path.join(
                output_dir, f"{base_name}.part-{chunk_num:03d}")

            # Write chunk to file
            with open(chunk_file, 'wb') as chunk_f:
                chunk_f.write(chunk)

            # Update progress
            bytes_processed += len(chunk)
            progress = bytes_processed / total_size * 100
            chunk_size_mb = len(chunk) / (1024 * 1024)

            logger.info(
                f"Created chunk {chunk_num}/{expected_chunks}: {chunk_file} ({chunk_size_mb:.2f} MB) - {progress:.1f}% complete")

    logger.info(
        f"File splitting complete. Created {chunk_num} chunks in {output_dir}")
    return chunk_num


def main():
    """Main function to parse arguments and split the file"""
    parser = argparse.ArgumentParser(
        description="Split large Elasticsearch data files into smaller chunks")
    parser.add_argument("--input", "-i", required=True,
                        help="Path to the input file to split")
    parser.add_argument("--output-dir", "-o", default="./split_data",
                        help="Directory to store the split files")
    parser.add_argument("--chunk-size", "-s", type=int, default=90,
                        help="Size of each chunk in MB (default: 90)")

    args = parser.parse_args()

    input_file = args.input
    output_dir = args.output_dir
    chunk_size = args.chunk_size

    # Validate input file
    if not os.path.exists(input_file):
        logger.error(f"Input file does not exist: {input_file}")
        return 1

    # Create output directory path
    output_path = Path(output_dir)
    if not output_path.exists():
        logger.info(f"Creating output directory: {output_dir}")
        output_path.mkdir(parents=True)

    # Split the file
    num_chunks = split_file(input_file, output_dir, chunk_size)

    # Create the reassembly scripts
    create_reassembly_scripts(input_file, output_dir, num_chunks)

    return 0


def create_reassembly_scripts(input_file, output_dir, num_chunks):
    """Create scripts to reassemble the chunks back into the original file"""
    base_name = os.path.basename(input_file)
    input_dir = os.path.dirname(input_file)

    # Create bash script
    bash_script = os.path.join(output_dir, "reassemble.sh")
    with open(bash_script, 'w') as f:
        f.write("#!/bin/bash\n\n")
        f.write(f"# Create directory if it doesn't exist\n")
        f.write(f"mkdir -p {input_dir}\n\n")
        f.write(f"# Reassemble file from chunks\n")
        f.write(f"cat {output_dir}/{base_name}.part-* > {input_file}\n\n")
        f.write(f'echo "File reassembled: {input_file}"\n')

    # Set execute permissions
    os.chmod(bash_script, 0o755)
    logger.info(f"Created bash reassembly script: {bash_script}")

    # Create PowerShell script for Windows users
    ps_script = os.path.join(output_dir, "reassemble.ps1")
    with open(ps_script, 'w') as f:
        f.write(f"# Create directory if it doesn't exist\n")
        f.write(
            f"New-Item -ItemType Directory -Force -Path \"{input_dir}\"\n\n")
        f.write(f"# Reassemble file from chunks\n")
        f.write(
            f"$chunks = Get-ChildItem -Path \"{output_dir}\" -Filter \"{base_name}.part-*\" | Sort-Object Name\n")
        f.write(f"$outputFile = \"{input_file}\"\n\n")
        f.write(f"# Remove output file if it exists\n")
        f.write(f"if (Test-Path $outputFile) {{\n")
        f.write(f"    Remove-Item -Path $outputFile\n")
        f.write(f"}}\n\n")
        f.write(f"# Combine all chunks\n")
        f.write(f"$chunks | ForEach-Object {{\n")
        f.write(f"    Get-Content $_.FullName -Raw -Encoding byte | Add-Content -Path $outputFile -Encoding byte\n")
        f.write(f"}}\n\n")
        f.write(f'Write-Host "File reassembled: {input_file}"\n')

    logger.info(f"Created PowerShell reassembly script: {ps_script}")


if __name__ == "__main__":
    main()
