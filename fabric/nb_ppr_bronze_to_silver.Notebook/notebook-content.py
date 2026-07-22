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

# Notebook: nb_ppr_bronze_to_silver

# Transform raw Property Price Register (PPR) data from the Bronze layer into a standardised Silver Delta dataset.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Value used only when running the notebook manually
load_date = "2026-07-11"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build the Bronze source path using the supplied load date
source_path = (
    f"Files/Bronze/PPR/Extracted/"
    f"load_date={load_date}/PPR-ALL.csv"
)

df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(source_path)
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Examine the schema Spark inferred

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean price field and convert to numeric
from pyspark.sql.functions import regexp_replace, col

df_clean = df.withColumn(
    "SalePrice",
    regexp_replace(col("Price (�)"), "€|,", "").cast("double")
)

display(df_clean.select("Price (�)", "SalePrice"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean price field (�) and convert to numeric
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import regexp_replace, col

df_clean = df.withColumn(
    "SalePrice",
    regexp_replace(
        col("Price (�)"),
        "[^0-9.]",
        ""
    ).cast(DecimalType(18,2))
)

display(
    df_clean.select(
        "Price (�)",
        "SalePrice"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert sale date to date datatype
from pyspark.sql.functions import to_date

df_clean = df_clean.withColumn(
    "SaleDate",
    to_date(
        col("Date of Sale (dd/mm/yyyy)"),
        "dd/MM/yyyy"
    )
)

display(
    df_clean.select(
        "Date of Sale (dd/mm/yyyy)",
        "SaleDate"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check updated schema
df_clean.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for invalid or missing sale dates
df_clean.filter(
    col("SaleDate").isNull()
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for invalid sale prices
df_clean.filter(
    col("SalePrice").isNull()
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create standardised Silver schema
df_silver = df_clean.select(
    "SaleDate",
    "Address",
    "County",
    "Eircode",
    "SalePrice",
    col("Not Full Market Price").alias("IsFullMarketPrice"),
    col("VAT Exclusive").alias("IsVATExclusive"),
    col("Description of Property").alias("PropertyType"),
    col("Property Size Description").alias("PropertySizeCategory")
)

display(df_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardise full market sale indicator
from pyspark.sql.functions import when

df_silver = df_silver.withColumn(
    "IsFullMarketPrice",
    when(
        col("IsFullMarketPrice") == "No",
        "Yes"
    ).otherwise("No")
)

# Review the transformation
display(
    df_silver.select("IsFullMarketPrice").limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check distribution of full market sales
df_silver.groupBy(
    "IsFullMarketPrice"
).count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Remove duplicate records
df_silver = df_silver.dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check record count
print(df_silver.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add load timestamp
from pyspark.sql.functions import current_timestamp

df_silver = df_silver.withColumn(
    "LoadTimestamp",
    current_timestamp()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add source system identifier
from pyspark.sql.functions import lit

df_silver = df_silver.withColumn(
    "SourceSystem",
    lit("PPR")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final check of schema
df_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardise property type values
from pyspark.sql.functions import when

df_silver = df_silver.withColumn(
    "PropertyType",
    when(
        col("PropertyType").contains("Ath"),
        "Second-Hand Dwelling house /Apartment"
    )
    .when(
        col("PropertyType").contains("Nua"),
        "New Dwelling house /Apartment"
    )
    .otherwise(col("PropertyType"))
)

# Review all property type values in Silver
display(
    df_silver
        .groupBy("PropertyType")
        .count()
        .orderBy("count", ascending=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Review sample records
display(df_silver.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace the existing Delta table schema to include load metadata and standardised Silver attributes
df_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .save("Files/Silver/ppr_property_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Silver Delta table
df_silver_check = spark.read.format("delta").load(
    "Files/Silver/ppr_property_sales"
)

display(df_silver_check.limit(20))

# Verify record count
print(df_silver_check.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
