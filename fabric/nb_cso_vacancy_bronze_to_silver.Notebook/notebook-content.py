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

# Notebook: nb_cso_vacancy_bronze_to_silver

# Transform raw CSO JSON-stat vacancy data into a structured Silver Delta dataset.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Imports
import json

from datetime import datetime, timezone
from itertools import product

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    to_date,
    to_timestamp
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Pipeline parameter
load_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resolve load date and Bronze file path
if not load_date:
    load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

bronze_path = (
    f"Files/Bronze/CSO/VAC14/Raw/"
    f"load_date={load_date}/VAC14.json"
)

print(f"Load date: {load_date}")
print(f"Bronze path: {bronze_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read raw CSO JSON-stat file
raw_text = (
    spark.read
        .option("wholetext", "true")
        .text(bronze_path)
        .first()["value"]
)

payload = json.loads(raw_text)

dataset = payload[0] if isinstance(payload, list) else payload

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect source dataset metadata
print(f"Dataset: {dataset.get('label', 'Unknown')}")
print(f"Source: {dataset.get('source', 'Central Statistics Office, Ireland')}")
print(f"Source updated: {dataset.get('updated', 'Not supplied')}")
print(f"Dimensions: {dataset.get('size')}")
print(f"Number of values: {len(dataset.get('value', []))}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract JSON-stat dimensions and values
dimensions = dataset["dimension"]

statistic_dimension = dimensions["STATISTIC"]
quarter_dimension = dimensions["TLIST(Q1)"]
local_authority_dimension = dimensions["C03789V04537"]

statistic_codes = statistic_dimension["category"]["index"]
quarter_codes = quarter_dimension["category"]["index"]
local_authority_codes = local_authority_dimension["category"]["index"]

statistic_labels = statistic_dimension["category"]["label"]
quarter_labels = quarter_dimension["category"]["label"]
local_authority_labels = local_authority_dimension["category"]["label"]

statistic_units = statistic_dimension["category"]["unit"]

values = dataset["value"]
source_updated = dataset.get("updated")

expected_value_count = (
    len(statistic_codes)
    * len(quarter_codes)
    * len(local_authority_codes)
)

print(f"Expected values: {expected_value_count}")
print(f"Actual values: {len(values)}")

if len(values) != expected_value_count:
    raise ValueError("JSON-stat dimensions do not match the number of values.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Flatten JSON-stat values into individual records
records = []

for value_position, (
    statistic_code,
    quarter_code,
    local_authority_code
) in enumerate(
    product(
        statistic_codes,
        quarter_codes,
        local_authority_codes
    )
):
    value = values[value_position]

    unit = (
        statistic_units
            .get(statistic_code, {})
            .get("label")
    )

    records.append(
        (
            statistic_code,
            statistic_labels[statistic_code],
            quarter_code,
            quarter_labels[quarter_code],
            local_authority_code,
            local_authority_labels[local_authority_code],
            unit,
            float(value) if value is not None else None,
            source_updated,
            load_date
        )
    )

print(f"Records created: {len(records)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define Silver dataset schema
silver_schema = StructType(
    [
        StructField("StatisticCode", StringType(), False),
        StructField("Statistic", StringType(), False),
        StructField("QuarterCode", StringType(), False),
        StructField("Quarter", StringType(), False),
        StructField("LocalAuthorityCode", StringType(), False),
        StructField("LocalAuthority", StringType(), False),
        StructField("Unit", StringType(), True),
        StructField("Value", DoubleType(), True),
        StructField("SourceUpdatedTimestamp", StringType(), True),
        StructField("LoadDate", StringType(), False)
    ]
)

df_silver = spark.createDataFrame(
    records,
    schema=silver_schema
)

df_silver = (
    df_silver
        .withColumn(
            "SourceUpdatedTimestamp",
            to_timestamp(
                col("SourceUpdatedTimestamp"),
                "yyyy-MM-dd'T'HH:mm:ss.SSSX"
            )
        )
        .withColumn(
            "LoadDate",
            to_date(col("LoadDate"))
        )
        .withColumn(
            "IngestedTimestamp",
            current_timestamp()
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate flattened Silver dataset
print(f"Silver row count: {df_silver.count()}")

display(
    df_silver
        .orderBy(
            "StatisticCode",
            "QuarterCode",
            "LocalAuthority"
        )
        .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save structured vacancy data to Silver Delta
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Files/Silver/cso_vacancy_local_authority")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read and validate Silver Delta dataset
df_silver_check = (
    spark.read
        .format("delta")
        .load("Files/Silver/cso_vacancy_local_authority")
)

print(f"Silver Delta row count: {df_silver_check.count()}")

display(
    df_silver_check
        .orderBy(
            "StatisticCode",
            "QuarterCode",
            "LocalAuthority"
        )
        .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
