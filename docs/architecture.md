# Architecture

## Purpose

This project demonstrates a simple Lakehouse data processing architecture using public API data.
The project follows a Medallion Architecture pattern.

## Source

The source system is the public PokeAPI.
The API provides JSON data about Pokémon, including:

- ID
- name
- height
- weight
- base experience
- types
- abilities
- source URLs

## Bronze layer

The Bronze layer stores raw API responses with minimal transformation.
The purpose of the Bronze layer is to preserve the source data as closely as possible.


## Silver layer

The Silver layer contains cleaned and structured Delta tables.
The raw nested JSON is converted into relational-style tables:

- `silver_pokemon`
- `silver_pokemon_types`
- `silver_pokemon_abilities`


## Gold layer

The Gold layer contains reporting-ready tables.

The Gold tables are:

- `gold_pokemon_type_summary`
- `gold_pokemon_profile`

These tables support simple reporting and analysis, such as:

- number of Pokémon by type
- average height by type
- average weight by type
- average base experience by type
- one-row-per-Pokémon profile reporting