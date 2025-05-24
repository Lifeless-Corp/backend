#!/bin/bash

# Create directory if it doesn't exist
mkdir -p index

# Reassemble file from chunks
cat index/split_data/pmc_articles_data.jsonl.part-* > index/pmc_articles_data.jsonl

echo "File reassembled: index/pmc_articles_data.jsonl"
