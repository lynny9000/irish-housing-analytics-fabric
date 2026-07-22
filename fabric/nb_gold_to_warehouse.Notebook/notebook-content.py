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
# META     }
# META   }
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Notebook: nb_gold_to_warehouse

# Transform Gold business datasets into warehouse dimensions and fact tables for analytical reporting.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# Imports
from pyspark.sql.functions import (
    col,
    when,
    lit,
    count,
    avg,
    min,
    max,
    current_timestamp,
    row_number,
    year,
    quarter,
    month,
    date_format
)

from pyspark.sql.types import DecimalType

from pyspark.sql.window import Window

from pyspark.sql.functions import monotonically_increasing_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Gold property sales table
df_gold = spark.read.format("delta").load(
    "Files/Gold/gold_property_sales"
)

display(df_gold.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build county dimension
dim_county = (
    df_gold
        .select("County")
        .distinct()
        .orderBy("County")
)

window_spec = Window.orderBy("County")

dim_county = dim_county.withColumn(
    "CountyKey",
    row_number().over(window_spec)
)

dim_county = dim_county.select(
    "CountyKey",
    dim_county["County"].alias("CountyName")
)

print(dim_county.count())
display(dim_county)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save county dimension to Lakehouse table
dim_county.write \
    .mode("overwrite") \
    .saveAsTable("stg_dim_county")

spark.sql("SELECT * FROM stg_dim_county").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build property type dimension
dim_property_type = (
    df_gold
        .select("PropertyType")
        .distinct()
        .orderBy("PropertyType")
)

window_spec = Window.orderBy("PropertyType")

dim_property_type = dim_property_type.withColumn(
    "PropertyTypeKey",
    row_number().over(window_spec)
)

dim_property_type = dim_property_type.select(
    "PropertyTypeKey",
    "PropertyType"
)

display(dim_property_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save property type dimension to Lakehouse table
dim_property_type.write \
    .mode("overwrite") \
    .saveAsTable("stg_dim_property_type")

spark.sql("SELECT * FROM stg_dim_property_type").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build sale type dimension
dim_sale_type = (
    df_gold
        .select("SaleType")
        .distinct()
        .orderBy("SaleType")
)

window_spec = Window.orderBy("SaleType")

dim_sale_type = dim_sale_type.withColumn(
    "SaleTypeKey",
    row_number().over(window_spec)
)

print(dim_sale_type.count())
display(dim_sale_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save sale type dimension to Lakehouse table
dim_sale_type.write \
    .mode("overwrite") \
    .saveAsTable("stg_dim_sale_type")

spark.sql("SELECT * FROM stg_dim_sale_type").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build the complete shared date dimension
calendar_dates = spark.sql(
    """
    SELECT explode(
        sequence(
            to_date('2010-01-01'),
            to_date('2030-12-31'),
            interval 1 day
        )
    ) AS FullDate
    """
)

dim_date = (
    calendar_dates
    .select(
        date_format(
            col("FullDate"),
            "yyyyMMdd"
        ).cast("int").alias("DateKey"),

        col("FullDate"),

        year(
            col("FullDate")
        ).cast("int").alias("Year"),

        quarter(
            col("FullDate")
        ).cast("int").alias("Quarter"),

        month(
            col("FullDate")
        ).cast("int").alias("Month"),

        date_format(
            col("FullDate"),
            "MMMM"
        ).alias("MonthName")
    )
    .orderBy("FullDate")
)

# Validate the complete shared calendar
print(f"Date-dimension rows: {dim_date.count():,}")

display(dim_date.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save date dimension to Lakehouse table
dim_date.write \
    .mode("overwrite") \
    .saveAsTable("stg_dim_date")

spark.sql("SELECT * FROM stg_dim_date").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate staging dimension tables
spark.sql("SHOW TABLES").show(100, False)

spark.sql("SELECT COUNT(*) FROM stg_dim_county").show()

spark.sql("SELECT COUNT(*) FROM stg_dim_property_type").show()

spark.sql("SELECT COUNT(*) FROM stg_dim_sale_type").show()

spark.sql("SELECT COUNT(*) FROM stg_dim_date").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load staging dimension tables to build fact_property_sales
stg_dim_county = spark.table("stg_dim_county")

stg_dim_property_type = spark.table("stg_dim_property_type")

stg_dim_sale_type = spark.table("stg_dim_sale_type")

stg_dim_date = spark.table("stg_dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build fact property sales
fact_property_sales = (
    df_gold

    .join(
        stg_dim_date,
        df_gold.SaleDate == stg_dim_date.FullDate,
        "left"
    )

    .join(
        stg_dim_county,
        df_gold.County == stg_dim_county.CountyName,
        "left"
    )

    .join(
        stg_dim_property_type,
        "PropertyType",
        "left"
    )

    .join(
        stg_dim_sale_type,
        "SaleType",
        "left"
    )
)

from pyspark.sql.functions import monotonically_increasing_id

fact_property_sales = fact_property_sales.select(
    monotonically_increasing_id().alias("PropertySaleKey"),
    "DateKey",
    "CountyKey",
    "PropertyTypeKey",
    "SaleTypeKey",
    "Address",
    "Eircode",
    "SalePrice",
    "IsFullMarketPrice",
    "LoadTimestamp"
)

print(fact_property_sales.count())
display(fact_property_sales.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save fact property sales table to Lakehouse
fact_property_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("stg_fact_property_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate fact property sales table
spark.sql("SELECT COUNT(*) FROM stg_fact_property_sales").show()

spark.sql("""
SELECT *
FROM stg_fact_property_sales
LIMIT 10
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
