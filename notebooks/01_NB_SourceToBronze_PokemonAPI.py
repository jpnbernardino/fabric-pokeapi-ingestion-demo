# 01_NB_SourceToBronze_PokemonAPI
#
# This notebook gets Pokemon data from the public PokeAPI and saves it into a Bronze (raw) table.
# ------------------------------------------------------------------------------------------------

# Imports
# --------

import requests
import json
import time
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit




# API settings
# -------------

base_url = "https://pokeapi.co/api/v2/pokemon"
pokemon_limit = 151
pokemon_offset = 0

bronze_table_name = "bronze_pokemon_raw"




# Get list of pokemon
# --------------------

params = {
    "offset": pokemon_offset,
    "limit": pokemon_limit
}

response = requests.get(base_url, params=params)
response.raise_for_status()

pokemon_list = response.json()
pokemon_results = pokemon_list["results"]

print("Pokemon records from list endpoint:", len(pokemon_results))




# Get the detailed result for each pokemon
# -----------------------------------------

rows = []
run_time = datetime.utcnow().isoformat()

for p in pokemon_results:
    pokemon_name = p["name"]
    pokemon_url = p["url"]

    detail_response = requests.get(pokemon_url)
    detail_response.raise_for_status()

    detail_json = detail_response.json()

    rows.append(
        Row(
            pokemon_id=detail_json.get("id"),
            pokemon_name=detail_json.get("name"),
            source_url=pokemon_url,
            raw_json=json.dumps(detail_json),
            source_system="PokeAPI",
            ingested_at_utc=run_time
        )
    )

    # small pause so the demo does not call the public API too fast
    time.sleep(0.05)


print("Pokemon detail records loaded:", len(rows))



# Create df
# -----------------------

bronze_df = spark.createDataFrame(rows)

bronze_df = (
    bronze_df
    .withColumn("bronze_loaded_at", current_timestamp())
    .withColumn("api_limit", lit(pokemon_limit))
    .withColumn("api_offset", lit(pokemon_offset))
)

bronze_df.printSchema()
display(bronze_df.limit(10))




# Save as Bronze delta table
# ---------------------------

bronze_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(bronze_table_name)

print("Saved Bronze table:", bronze_table_name)


# ---------------------------------------------------------------------------------------------------------


# Simple count check
# -------------------

bronze_count = spark.table(bronze_table_name).count()

print("Bronze count:", bronze_count)

if bronze_count != len(rows):
    raise Exception("Bronze count does not match expected row count")

print("Notebook 1 complete")

