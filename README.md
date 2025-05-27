# Football Data Engineering Project

## Overview
This project demonstrates an end-to-end data engineering pipeline using AWS services including S3, Glue, EMR, Athena, and Crawlers. The data is sourced from football match results over the past 4 years.

## Pipeline Steps
1. **Raw Ingestion**: Upload raw CSVs to `s3://football-project/collect/raw/`
2. **Glue Cleaning Job**: Cleans and stores to `s3://football-project/translate/cleaned/`
3. **EMR Transformations**: Advanced processing, output to `s3://football-project/curated/match_parquet/`
4. **Athena Queries**: Use Glue Crawler to catalog and Athena to analyze.

## Folder Structure
- `scripts/` - Python scripts for Glue and EMR jobs
- `athena_queries/` - SQL queries for Athena
- `sample_data/` - Mock CSV files to simulate football match data

## Sample S3 Paths
- Raw Data: `s3://football-project/collect/raw/`
- Cleaned Data: `s3://football-project/translate/cleaned/`
- Curated Parquet Data: `s3://football-project/curated/match_parquet/`

## Author
Sankhadip Dey
