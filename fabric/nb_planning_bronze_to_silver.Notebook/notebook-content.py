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

# Notebook: nb_planning_bronze_to_silver

# Transform normalised Irish planning-application extracts from the Bronze layer into a cleaned and consolidated Silver Delta table.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Imports
from datetime import datetime, timezone

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
    date_format,
    trim
)

from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pipeline parameter with today's UTC date as the manual-run default
load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

print(f"Load date: {load_date}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build the Bronze path for the selected load date
bronze_base_path = "Files/Bronze/PlanningSQL"

# Read the main planning-application extract
df_planning_application = spark.read.format("parquet").load(
    f"{bronze_base_path}/PlanningApplication/load_date={load_date}/PlanningApplication.parquet"
)

# Read the planning-authority reference extract
df_planning_authority = spark.read.format("parquet").load(
    f"{bronze_base_path}/PlanningAuthority/load_date={load_date}/PlanningAuthority.parquet"
)

# Read the application-type reference extract
df_application_type = spark.read.format("parquet").load(
    f"{bronze_base_path}/ApplicationType/load_date={load_date}/ApplicationType.parquet"
)

# Read the application-status reference extract
df_application_status = spark.read.format("parquet").load(
    f"{bronze_base_path}/ApplicationStatus/load_date={load_date}/ApplicationStatus.parquet"
)

# Read the decision-type reference extract
df_decision_type = spark.read.format("parquet").load(
    f"{bronze_base_path}/DecisionType/load_date={load_date}/DecisionType.parquet"
)

# Read the land-use-type reference extract
df_land_use_type = spark.read.format("parquet").load(
    f"{bronze_base_path}/LandUseType/load_date={load_date}/LandUseType.parquet"
)

# Display a sample of the main planning-application data
display(df_planning_application.limit(10))

# Validate the number of records read from each Bronze extract
print(f"PlanningApplication: {df_planning_application.count():,}")
print(f"PlanningAuthority: {df_planning_authority.count():,}")
print(f"ApplicationType: {df_application_type.count():,}")
print(f"ApplicationStatus: {df_application_status.count():,}")
print(f"DecisionType: {df_decision_type.count():,}")
print(f"LandUseType: {df_land_use_type.count():,}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect the main planning-application schema
df_planning_application.printSchema()

# Inspect the reference-table column names
print("PlanningAuthority:", df_planning_authority.columns)
print("ApplicationType:", df_application_type.columns)
print("ApplicationStatus:", df_application_status.columns)
print("DecisionType:", df_decision_type.columns)
print("LandUseType:", df_land_use_type.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Join the normalised planning tables into one analytical dataset
df_planning_joined = (
    df_planning_application
    .join(
        df_planning_authority,
        on="PlanningAuthorityID",
        how="left"
    )
    .join(
        df_application_type,
        on="ApplicationTypeID",
        how="left"
    )
    .join(
        df_application_status,
        on="ApplicationStatusID",
        how="left"
    )
    .join(
        df_decision_type,
        on="DecisionTypeID",
        how="left"
    )
    .join(
        df_land_use_type,
        on="LandUseTypeID",
        how="left"
    )
)

# Display the joined planning data
display(
    df_planning_joined.select(
        "SourceObjectID",
        "ApplicationNumber",
        "PlanningAuthorityName",
        "ApplicationTypeName",
        "ApplicationStatusName",
        "DecisionTypeName",
        "LandUseTypeName",
        "ReceivedDate",
        "Latitude",
        "Longitude"
    ).limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate the joined row count
source_row_count = df_planning_application.count()
joined_row_count = df_planning_joined.count()

print(f"Source rows: {source_row_count:,}")
print(f"Joined rows: {joined_row_count:,}")
print(f"Row-count difference: {joined_row_count - source_row_count:,}")

# Check for unmatched reference records
print(
    "Missing planning authorities:",
    df_planning_joined.filter(
        col("PlanningAuthorityName").isNull()
    ).count()
)

print(
    "Missing application types:",
    df_planning_joined.filter(
        col("ApplicationTypeName").isNull()
    ).count()
)

print(
    "Missing application statuses:",
    df_planning_joined.filter(
        col("ApplicationStatusName").isNull()
    ).count()
)

print(
    "Missing decision types:",
    df_planning_joined.filter(
        col("DecisionTypeName").isNull()
    ).count()
)

print(
    "Missing land-use types:",
    df_planning_joined.filter(
        col("LandUseTypeName").isNull()
    ).count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Distinguish valid source nulls from broken reference-table matches
reference_checks = [
    ("Application status", "ApplicationStatusID", "ApplicationStatusName"),
    ("Decision type", "DecisionTypeID", "DecisionTypeName"),
    ("Land-use type", "LandUseTypeID", "LandUseTypeName")
]

for label, id_column, name_column in reference_checks:
    null_source_ids = df_planning_joined.filter(
        col(id_column).isNull()
    ).count()

    unmatched_ids = df_planning_joined.filter(
        col(id_column).isNotNull() &
        col(name_column).isNull()
    ).count()

    print(f"{label} - null source IDs: {null_source_ids:,}")
    print(f"{label} - unmatched non-null IDs: {unmatched_ids:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for missing source record identifiers
missing_source_ids = df_planning_joined.filter(
    col("SourceObjectID").isNull()
).count()

# Check for duplicate source record identifiers
duplicate_source_ids = (
    df_planning_joined
    .groupBy("SourceObjectID")
    .count()
    .filter(col("count") > 1)
    .count()
)

# Check for missing application numbers
missing_application_numbers = df_planning_joined.filter(
    col("ApplicationNumber").isNull()
).count()

# Check for coordinates outside the approximate Irish geographic range
invalid_coordinates = df_planning_joined.filter(
    col("Latitude").isNull() |
    col("Longitude").isNull() |
    ~col("Latitude").between(51.0, 56.0) |
    ~col("Longitude").between(-11.0, -5.0)
).count()

print(f"Missing source IDs: {missing_source_ids:,}")
print(f"Duplicate source IDs: {duplicate_source_ids:,}")
print(f"Missing application numbers: {missing_application_numbers:,}")
print(f"Missing or invalid coordinates: {invalid_coordinates:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the cleaned and consolidated Silver planning dataset
df_planning_silver = df_planning_joined.select(
    col("SourceObjectID"),
    trim(col("ApplicationNumber")).alias("ApplicationNumber"),

    col("PlanningAuthorityID"),
    trim(col("PlanningAuthorityName")).alias("PlanningAuthority"),

    col("ApplicationTypeID"),
    trim(col("ApplicationTypeName")).alias("ApplicationType"),

    col("ApplicationStatusID"),
    when(
        col("ApplicationStatusName").isNull(),
        lit("Not recorded")
    ).otherwise(
        trim(col("ApplicationStatusName"))
    ).alias("ApplicationStatus"),

    col("DecisionTypeID"),
    when(
        col("DecisionTypeName").isNull(),
        lit("Not recorded")
    ).otherwise(
        trim(col("DecisionTypeName"))
    ).alias("DecisionType"),

    col("LandUseTypeID"),
    when(
        col("LandUseTypeName").isNull(),
        lit("Not recorded")
    ).otherwise(
        trim(col("LandUseTypeName"))
    ).alias("LandUseType"),

    trim(col("DevelopmentDescription")).alias("DevelopmentDescription"),
    trim(col("DevelopmentAddress")).alias("DevelopmentAddress"),
    trim(col("DevelopmentPostcode")).alias("DevelopmentPostcode"),

    col("ITMEasting"),
    col("ITMNorthing"),
    col("Latitude"),
    col("Longitude"),
    col("AreaOfSite"),
    col("NumResidentialUnits"),
    col("OneOffHouse"),
    col("FloorArea"),

    col("ReceivedDate").cast("date").alias("ReceivedDate"),
    col("DecisionDate").cast("date").alias("DecisionDate"),
    col("GrantDate").cast("date").alias("GrantDate"),
    col("ExpiryDate").cast("date").alias("ExpiryDate"),

    trim(col("LinkAppDetails")).alias("ApplicationDetailsURL"),
    col("SourceETLDate"),
    col("InsertedAt").alias("SQLInsertedAt"),
    col("RecordLastUpdatedAt").alias("SQLRecordLastUpdatedAt"),

    lit(load_date).cast("date").alias("LoadDate"),
    current_timestamp().alias("SilverLoadTimestamp"),
    lit("Irish Planning Applications").alias("SourceSystem")
)

# Display the cleaned Silver dataset
display(df_planning_silver.limit(20))

# Verify the Silver row count
print(f"Silver row count: {df_planning_silver.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge new and changed planning applications into the Silver Delta table
silver_path = "Files/Silver/planning_applications"

incremental_row_count = df_planning_silver.count()

if DeltaTable.isDeltaTable(spark, silver_path):
    silver_delta = DeltaTable.forPath(spark, silver_path)

    silver_delta.alias("target") \
        .merge(
            df_planning_silver.alias("source"),
            "target.SourceObjectID = source.SourceObjectID"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    print(f"Silver Delta merge completed: {incremental_row_count:,} source rows processed.")

else:
    df_planning_silver.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .format("delta") \
        .save(silver_path)

    print(f"Silver Delta table created: {incremental_row_count:,} rows.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the Silver Delta table back from OneLake
df_planning_silver_check = spark.read.format("delta").load(
    "Files/Silver/planning_applications"
)

# Display a sample of the saved Silver data
display(df_planning_silver_check.limit(20))

# Verify the saved Silver row count
print(f"Saved Silver row count: {df_planning_silver_check.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
