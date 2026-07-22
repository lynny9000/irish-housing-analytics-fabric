# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4a3c4a73-0851-4a19-a9df-7af623f48047",
# META       "default_lakehouse_name": "lhirishhousingdev",
# META       "default_lakehouse_workspace_id": "e0868319-d72d-49ff-83b5-426523564ec8",
# META       "known_lakehouses": [
# META         {
# META           "id": "4a3c4a73-0851-4a19-a9df-7af623f48047"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Notebook: nb_cso_vacancy_gold_to_warehouse

# Prepare Gold county vacancy metrics as Lakehouse staging tables for the Fabric Warehouse.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Imports
from pyspark.sql.functions import (
    col,
    row_number,
    current_timestamp
)

from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Gold housing vacancy Delta dataset
df_gold = (
    spark.read
        .format("delta")
        .load("Files/Gold/gold_housing_vacancy")
)

print(f"Gold row count: {df_gold.count()}")

display(
    df_gold
        .orderBy(
            "QuarterCode",
            "County"
        )
        .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read existing county dimension staging table
stg_dim_county = spark.table(
    "stg_dim_county"
)

print(
    f"County dimension row count: "
    f"{stg_dim_county.count()}"
)

display(stg_dim_county)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build quarter dimension
stg_dim_quarter = (
    df_gold
        .select(
            "QuarterCode",
            "Quarter",
            "QuarterStartDate",
            "Year",
            "QuarterNumber"
        )
        .distinct()
        .withColumn(
            "QuarterKey",
            col("QuarterCode").cast("int")
        )
        .select(
            "QuarterKey",
            "Quarter",
            "QuarterStartDate",
            "Year",
            "QuarterNumber"
        )
        .orderBy("QuarterKey")
)

print(
    f"Quarter dimension row count: "
    f"{stg_dim_quarter.count()}"
)

display(stg_dim_quarter)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save quarter dimension as Lakehouse staging table
stg_dim_quarter.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("stg_dim_quarter")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate quarter dimension staging table
spark.sql("""
SELECT *
FROM stg_dim_quarter
ORDER BY QuarterKey
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build housing vacancy fact staging table
stg_fact_housing_vacancy = (
    df_gold.alias("gold")
    .join(
        stg_dim_county.alias("county"),
        col("gold.County") == col("county.CountyName"),
        "left"
    )
    .withColumn(
        "QuarterKey",
        col("gold.QuarterCode").cast("int")
    )
    .withColumn(
        "HousingVacancyKey",
        (
            col("QuarterKey") * 100
            + col("county.CountyKey")
        ).cast("long")
    )
    .select(
        "HousingVacancyKey",
        "QuarterKey",
        col("county.CountyKey").alias("CountyKey"),
        col("gold.VacantDwellings").alias("VacantDwellings"),
        col("gold.DwellingStock").alias("DwellingStock"),
        col("gold.VacancyRate")
            .cast("decimal(5,1)")
            .alias("VacancyRate"),
        col("gold.SourceUpdatedTimestamp")
            .alias("SourceUpdatedTimestamp"),
        col("gold.LoadDate").alias("LoadDate"),
        col("gold.GoldLoadTimestamp")
            .alias("LoadTimestamp")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate housing vacancy fact staging data
fact_row_count = (
    stg_fact_housing_vacancy.count()
)

missing_county_keys = (
    stg_fact_housing_vacancy
        .filter(col("CountyKey").isNull())
        .count()
)

duplicate_fact_keys = (
    stg_fact_housing_vacancy
        .groupBy("HousingVacancyKey")
        .count()
        .filter(col("count") > 1)
        .count()
)

print(f"Fact row count: {fact_row_count}")
print(f"Missing county keys: {missing_county_keys}")
print(f"Duplicate fact keys: {duplicate_fact_keys}")

display(
    stg_fact_housing_vacancy
        .orderBy(
            "QuarterKey",
            "CountyKey"
        )
        .limit(30)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save housing vacancy fact as Lakehouse staging table
stg_fact_housing_vacancy.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("stg_fact_housing_vacancy")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate warehouse staging tables
spark.sql("""
SELECT COUNT(*) AS QuarterCount
FROM stg_dim_quarter
""").show()

spark.sql("""
SELECT COUNT(*) AS HousingVacancyCount
FROM stg_fact_housing_vacancy
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
