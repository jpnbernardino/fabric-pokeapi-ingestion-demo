# 03_NB_SilverToGold_PokemonAPI
#
# This notebook creates a couple of Gold tables from the Silver Pokemon tables.
# -----------------------------------------------------------------------------


# Imports
# --------

from pyspark.sql.functions import (col, count, countDistinct, avg, min, max, round, current_timestamp)




# Settings
# ---------

silver_pokemon_table = "silver_pokemon"
silver_types_table = "silver_pokemon_types"
silver_abilities_table = "silver_pokemon_abilities"

gold_type_summary_table = "gold_pokemon_type_summary"
gold_profile_table = "gold_pokemon_profile"




# Read Silver tables
# --------------------

pokemon_df = spark.table(silver_pokemon_table)
types_df = spark.table(silver_types_table)
abilities_df = spark.table(silver_abilities_table)

print("Pokemon rows:", pokemon_df.count())
print("Type rows:", types_df.count())
print("Ability rows:", abilities_df.count())




# Gold table 1: summary by type
# ------------------------------

type_summary_df = (
    types_df
    .join(pokemon_df, on="pokemon_id", how="inner")
    .groupBy("type_name")
    .agg(
        countDistinct("pokemon_id").alias("pokemon_count"),
        round(avg("height"), 2).alias("avg_height"),
        round(avg("weight"), 2).alias("avg_weight"),
        round(avg("base_experience"), 2).alias("avg_base_experience"),
        min("base_experience").alias("min_base_experience"),
        max("base_experience").alias("max_base_experience")
    )
    .withColumn("gold_loaded_at", current_timestamp())
)

display(type_summary_df)

type_summary_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_type_summary_table)

print("Saved:", gold_type_summary_table)





# Gold table 2: one row per pokemon
# -----------------------------------

type_count_df = (
    types_df
    .groupBy("pokemon_id")
    .agg(count("type_name").alias("type_count"))
)

ability_count_df = (
    abilities_df
    .groupBy("pokemon_id")
    .agg(
        count("ability_name").alias("ability_count"),
        countDistinct("ability_name").alias("distinct_ability_count")
    )
)

pokemon_profile_df = (
    pokemon_df
    .join(type_count_df, on="pokemon_id", how="left")
    .join(ability_count_df, on="pokemon_id", how="left")
    .select(
        col("pokemon_id"),
        col("pokemon_name"),
        col("base_experience"),
        col("height"),
        col("weight"),
        col("is_default"),
        col("type_count"),
        col("ability_count"),
        col("distinct_ability_count"),
        col("source_system"),
        col("source_url"),
        col("ingested_at_utc")
    )
    .withColumn("gold_loaded_at", current_timestamp())
)

display(pokemon_profile_df.limit(20))

pokemon_profile_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_profile_table)

print("Saved:", gold_profile_table)


# ---------------------------------------------------------------------------------------------------------


# Simple validation
# -------------------

profile_count = spark.table(gold_profile_table).count()
pokemon_count = pokemon_df.count()

print("Gold profile count:", profile_count)
print("Silver pokemon count:", pokemon_count)

if profile_count != pokemon_count:
    raise Exception("Gold profile count does not match silver pokemon count")

print("Notebook 3 complete")

