# Create directory if it doesn't exist
New-Item -ItemType Directory -Force -Path "index"

# Reassemble file from chunks
$chunks = Get-ChildItem -Path "index/split_data" -Filter "pmc_articles_data.jsonl.part-*" | Sort-Object Name
$outputFile = "index/pmc_articles_data.jsonl"

# Remove output file if it exists
if (Test-Path $outputFile) {
    Remove-Item -Path $outputFile
}

# Combine all chunks
$chunks | ForEach-Object {
    Get-Content $_.FullName -Raw -Encoding byte | Add-Content -Path $outputFile -Encoding byte
}

Write-Host "File reassembled: index/pmc_articles_data.jsonl"
