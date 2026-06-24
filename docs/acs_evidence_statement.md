# ACS Professional Currency Evidence Statement

## Artefact title

Microsoft Fabric Lakehouse API Ingestion and Transformation Demo

## Evidence category

Professional Work Artefact

## Summary

This artefact is a personal demonstration project created to show current skills in data orchestration using PySpark - API ingestion, data transformation, Lakehouse data processing, and reporting table preparation.

The project uses public data from the PokeAPI and follows a Medallion Architecture (Bronze, Silver, and Gold) data processing pattern.

## Skills demonstrated

This artefact demonstrates the following skills:

- REST API ingestion using PySpark
- JSON response handling
- PySpark transformation logic
- nested JSON processing and array flattening
- validation checks
- documentation of architecture and data flow

## Relevance to nominated occupation

The artefact aims to prove relevant skills to the Developer Programmer ANZSCO code (261312)

## Project components

The project contains three notebooks:

1. `01_NB_SourceToBronze_PokemonAPI.py`
   - Ingests data from a public REST API.
   - Saves raw JSON files to a Bronze layer.

2. `02_NB_BronzeToSilver_PokemonAPI.py`
   - Reads raw JSON data.
   - Creates curated Silver Delta tables.
   - Flattens nested arrays for Pokémon types and abilities.

3. `03_NB_SilverToGold_PokemonAPI.py`
   - Creates Gold reporting tables.
   - Produces type-level and Pokémon-level summary outputs.

## Statement of authorship

This artefact was created by me as a personal professional demonstration project.

The project was created independently and does not include confidential material from any employer or client.