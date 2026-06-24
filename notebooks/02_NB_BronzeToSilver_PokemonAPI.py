# 02_NB_BronzeToSilver_PokemonAPI
#
# This notebook reads the Bronze Pokemon table and turns the raw JSON into cleansed Silver tables.
# ------------------------------------------------------------------------------------------------

# Imports
# --------

from pyspark.sql.functions import col, from_json, explode_outer, current_timestamp, countDistinct

from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, BooleanType, ArrayType)


# Settings
# ---------

bronze_table = "bronze_pokemon_raw"

silver_pokemon_table = "silver_pokemon"
silver_types_table = "silver_pokemon_types"
silver_abilities_table = "silver_pokemon_abilities"



# Defining the schema
# --------------------

pokemon_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("base_experience", IntegerType(), True),
    StructField("height", IntegerType(), True),
    StructField("weight", IntegerType(), True),
    StructField("is_default", BooleanType(), True),
    StructField("order", IntegerType(), True),

    StructField("types", ArrayType(
        StructType([
            StructField("slot", IntegerType(), True),
            StructField("type", StructType([
                StructField("name", StringType(), True),
                StructField("url", StringType(), True)
            ]), True)
        ])
    ), True),

    StructField("abilities", ArrayType(
        StructType([
            StructField("is_hidden", BooleanType(), True),
            StructField("slot", IntegerType(), True),
            StructField("ability", StructType([
                StructField("name", StringType(), True),
                StructField("url", StringType(), True)
            ]), True)
        ])
    ), True)
])



# Read from Bronze
# ------------

bronze_df = spark.table(bronze_table)

print("Bronze row count:", bronze_df.count())
bronze_df.printSchema()




# Parse the JSON column
# -----------------------

parsed_df = bronze_df.withColumn(
    "pokemon_json",
    from_json(col("raw_json"), pokemon_schema)
)

parsed_df.printSchema()




# Main pokemon table
# --------------------

silver_pokemon_df = (
    parsed_df
    .select(
        col("pokemon_json.id").alias("pokemon_id"),
        col("pokemon_json.name").alias("pokemon_name"),
        col("pokemon_json.base_experience").alias("base_experience"),
        col("pokemon_json.height").alias("height"),
        col("pokemon_json.weight").alias("weight"),
        col("pokemon_json.is_default").alias("is_default"),
        col("pokemon_json.order").alias("pokemon_order"),
        col("source_system"),
        col("source_url"),
        col("ingested_at_utc")
    )
    .withColumn("silver_loaded_at", current_timestamp())
)

display(silver_pokemon_df.limit(10))

silver_pokemon_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_pokemon_table)

print("Saved:", silver_pokemon_table)




# Pokemon types table
# --------------------

silver_types_df = (
    parsed_df
    .select(
        col("pokemon_json.id").alias("pokemon_id"),
        col("pokemon_json.name").alias("pokemon_name"),
        explode_outer(col("pokemon_json.types")).alias("type_data"),
        col("ingested_at_utc")
    )
    .select(
        col("pokemon_id"),
        col("pokemon_name"),
        col("type_data.slot").alias("type_slot"),
        col("type_data.type.name").alias("type_name"),
        col("type_data.type.url").alias("type_url"),
        col("ingested_at_utc")
    )
    .withColumn("silver_loaded_at", current_timestamp())
)

display(silver_types_df.limit(10))

silver_types_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_types_table)

print("Saved:", silver_types_table)




# Pokemon abilities table
# -------------------------

silver_abilities_df = (
    parsed_df
    .select(
        col("pokemon_json.id").alias("pokemon_id"),
        col("pokemon_json.name").alias("pokemon_name"),
        explode_outer(col("pokemon_json.abilities")).alias("ability_data"),
        col("ingested_at_utc")
    )
    .select(
        col("pokemon_id"),
        col("pokemon_name"),
        col("ability_data.slot").alias("ability_slot"),
        col("ability_data.is_hidden").alias("is_hidden_ability"),
        col("ability_data.ability.name").alias("ability_name"),
        col("ability_data.ability.url").alias("ability_url"),
        col("ingested_at_utc")
    )
    .withColumn("silver_loaded_at", current_timestamp())
)

display(silver_abilities_df.limit(10))

silver_abilities_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_abilities_table)

print("Saved:", silver_abilities_table)


# ---------------------------------------------------------------------------------------------------------

# Basic checks
# --------------------

pokemon_count = spark.table(silver_pokemon_table).count()
types_count = spark.table(silver_types_table).count()
abilities_count = spark.table(silver_abilities_table).count()

print("Silver Pokemon count:", pokemon_count)
print("Silver Types count:", types_count)
print("Silver Abilities count:", abilities_count)

distinct_types = (
    spark.table(silver_types_table)
    .select(countDistinct("pokemon_id").alias("cnt"))
    .collect()[0]["cnt"]
)

print("Pokemon with type records:", distinct_types)

print("Notebook 2 complete")

