# ACS Professional Currency Evidence Statement

## Artefact title

Microsoft Fabric Lakehouse API Ingestion and Transformation Demo

## Evidence category

Professional Artefact

## Summary

This artefact is a personal demonstration project created to show current professional skills in API ingestion, PySpark data transformation, Lakehouse data processing, Delta table creation, and reporting table preparation.

The project uses public data from the PokeAPI and follows a Bronze, Silver, and Gold data processing pattern.

## Reason for using a personal demonstration project

In my professional work, I develop data ingestion, transformation, and reporting solutions using technologies such as SQL, PySpark, Microsoft Fabric, data warehousing tools, and BI platforms.

However, production scripts and database logic written for employers may contain confidential business logic, internal system details, client data, or employer-owned intellectual property. For that reason, this artefact was created independently using public data instead of employer-owned scripts.

This project does not contain:

- employer-owned code
- production schemas
- internal database names
- internal server names
- credentials
- client data
- resident or patient data
- confidential business rules
- proprietary transformation logic
- private API endpoints

## Skills demonstrated

This artefact demonstrates the following skills:

- REST API ingestion using Python
- JSON response handling
- raw data landing into a Bronze layer
- PySpark transformation logic
- nested JSON processing
- array flattening
- Delta table creation
- Silver layer curation
- Gold layer reporting table preparation
- validation checks
- documentation of architecture and data flow

## Relevance to nominated occupation

The artefact is relevant to ICT roles involving software development, data engineering, database development, analytics engineering, and business intelligence development.

The project demonstrates the ability to design and implement a small end-to-end data processing workflow from source API ingestion through to reporting-ready tables.

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
   - Performs validation checks.

## Statement of authorship

This artefact was created by me as a personal professional demonstration project.

The project was created independently and does not include confidential material from any employer or client.