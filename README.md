# Fabric PokeAPI Lakehouse Demo

## Overview

This repository contains a personal Microsoft Fabric Lakehouse demonstration project using public data from the PokeAPI.

The project demonstrates a simple Bronze, Silver, and Gold data processing pattern:

1. Source to Bronze:
   - Calls a public REST API.
   - Saves raw API responses as JSON files.
   - Adds basic ingestion metadata.

2. Bronze to Silver:
   - Reads raw JSON using PySpark.
   - Selects and standardises useful attributes.
   - Flattens nested arrays such as Pokémon types and abilities.
   - Writes curated Delta tables.

3. Silver to Gold:
   - Creates reporting-ready aggregate tables.
   - Produces type-level and profile-level summaries.
   - Adds validation checks and row count outputs.


## Technologies demonstrated

- Microsoft Fabric Lakehouse concepts
- PySpark
- REST API ingestion
- JSON processing
- Bronze/Silver/Gold data layering
- Delta table writes
- Dimensional/reporting-style modelling

## Project flow

```text
PokeAPI REST API
      |
      v
Bronze JSON files
      |
      v
Silver Delta tables
      |
      v
Gold reporting tables