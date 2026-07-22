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

# Notebook: nb_ppr_silver_to_gold

# Transform standardised Silver Property Price Register (PPR) sales data into Gold business analytics datasets.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
    current_timestamp
)

from pyspark.sql.types import DecimalType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Silver Delta table
df_silver = spark.read.format("delta").load(
    "Files/Silver/ppr_property_sales"
)

display(df_silver.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create canonical Gold property sales dataset
from pyspark.sql.functions import when

df_gold_property_sales = (
    df_silver
    .withColumn(
        "SaleType",
        when(
            col("IsVATExclusive") == True,
            "VAT Exclusive"
        ).otherwise(
            "VAT Inclusive"
        )
    )
    .select(
        "SaleDate",
        "County",
        "PropertyType",
        "PropertySizeCategory",
        "SaleType",
        "SalePrice",
        "IsFullMarketPrice",
        "Eircode",
        "Address",
        "LoadTimestamp",
        "SourceSystem"
    )
)

display(df_gold_property_sales.limit(20))

print(df_gold_property_sales.count())
print(df_gold_property_sales.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write Gold business transaction table
df_gold_property_sales.write \
    .mode("overwrite") \
    .format("delta") \
    .save("Files/Gold/gold_property_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import aggregation functions
from pyspark.sql.functions import count, avg, min, max

# Import round function
from pyspark.sql.functions import round

# Create county-level business metrics
df_gold_county = df_silver.groupBy(
    "County"
).agg(
    count("*").alias("SalesCount"),
    round(avg("SalePrice"), 2).alias("AverageSalePrice"),
    min("SalePrice").alias("MinimumSalePrice"),
    max("SalePrice").alias("MaximumSalePrice")
)

display(df_gold_county)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check number of counties
print(df_gold_county.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import col function
from pyspark.sql.functions import col

# Check for missing counties
df_gold_county.filter(
    col("County").isNull()
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Highest average house prices
display(
    df_gold_county.orderBy(
        "AverageSalePrice",
        ascending=False
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write Gold county metrics as Delta table
df_gold_county.write \
    .mode("overwrite") \
    .format("delta") \
    .save("Files/Gold/gold_county_metrics")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import year function
from pyspark.sql.functions import col

# Import year function
from pyspark.sql.functions import year

# Extract year from sale date
df_sales_by_year = df_silver.withColumn(
    "SaleYear",
    year("SaleDate")
)

# Create annual housing market metrics
df_gold_year = df_sales_by_year.groupBy(
    "SaleYear"
).agg(
    count("*").alias("SalesCount"),
    avg("SalePrice").alias("AverageSalePrice"),
    min("SalePrice").alias("MinimumSalePrice"),
    max("SalePrice").alias("MaximumSalePrice")
)

# Display annual housing metrics
display(
    df_gold_year.orderBy("SaleYear")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write annual housing market metrics as Delta table
df_gold_year.write \
    .mode("overwrite") \
    .format("delta") \
    .save("Files/Gold/gold_avg_price_by_year")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create analytical year field
df_sales_analysis = df_silver.withColumn(
    "SaleYear",
    year("SaleDate")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create county-year business metrics
df_gold_county_year = df_sales_analysis.groupBy(
    "County",
    "SaleYear"
).agg(
    count("*").alias("SalesCount"),
    avg("SalePrice").alias("AverageSalePrice"),
    min("SalePrice").alias("MinimumSalePrice"),
    max("SalePrice").alias("MaximumSalePrice")
)

display(
    df_gold_county_year.orderBy(
        "County",
        "SaleYear"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the number of aggregated county-year business records
print(df_gold_county_year.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write county-year business metrics as Delta table
df_gold_county_year.write \
    .mode("overwrite") \
    .format("delta") \
    .save("Files/Gold/gold_county_year_metrics")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create property type business metrics
df_gold_property_type = df_silver.groupBy(
    "PropertyType"
).agg(
    count("*").alias("SalesCount"),
    avg("SalePrice").alias("AverageSalePrice"),
    min("SalePrice").alias("MinimumSalePrice"),
    max("SalePrice").alias("MaximumSalePrice")
)

display(df_gold_property_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the number of property categories
print(df_gold_property_type.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write property type metrics as Delta table
df_gold_property_type.write \
    .mode("overwrite") \
    .format("delta") \
    .save("Files/Gold/gold_property_type_metrics")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create market summary metrics
df_gold_market_summary = df_silver.agg(
    count("*").alias("TotalSales"),
    avg("SalePrice").alias("AverageSalePrice"),
    min("SalePrice").alias("MinimumSalePrice"),
    max("SalePrice").alias("MaximumSalePrice")
)

display(df_gold_market_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate business summary values
county_count = df_silver.select(
    "County"
).distinct().count()

property_type_count = df_silver.select(
    "PropertyType"
).distinct().count()

print(f"Counties: {county_count}")
print(f"Property Types: {property_type_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create market summary metrics
df_gold_market_summary = df_silver.agg(
    count("*").alias("TotalSales"),
    avg("SalePrice").alias("AverageSalePrice"),
    min("SalePrice").alias("MinimumSalePrice"),
    max("SalePrice").alias("MaximumSalePrice")
)

# Review market summary metrics
display(df_gold_market_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write market summary metrics as Delta table
df_gold_market_summary.write \
    .mode("overwrite") \
    .format("delta") \
    .save("Files/Gold/gold_market_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
