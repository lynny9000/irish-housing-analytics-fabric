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

# Notebook: nb_planning_gold_to_warehouse

# Transform the Gold planning dataset into dimensional staging tables for loading into the Fabric Warehouse.

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
    current_timestamp,
    row_number,
    date_format,
    substring
)

from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the Gold planning-applications Delta table
df_gold = spark.read.format("delta").load(
    "Files/Gold/gold_planning_applications"
)

# Display a sample of the Gold data
display(df_gold.limit(10))

# Verify the Gold source row count
print(f"Gold source row count: {df_gold.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the planning-authority staging dimension
df_stg_dim_planning_authority = (
    df_gold
    .filter(
        col("PlanningAuthorityID").isNotNull()
    )
    .select(
        col("PlanningAuthorityID")
            .cast("int")
            .alias("PlanningAuthorityKey"),

        col("PlanningAuthority")
            .alias("PlanningAuthorityName")
    )
    .dropDuplicates(
        ["PlanningAuthorityKey"]
    )
    .orderBy(
        "PlanningAuthorityKey"
    )
)

# Display and validate the planning-authority dimension
display(df_stg_dim_planning_authority)

print(
    f"Planning-authority rows: "
    f"{df_stg_dim_planning_authority.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the application-type staging dimension
df_stg_dim_application_type = (
    df_gold
    .filter(
        col("ApplicationTypeID").isNotNull()
    )
    .select(
        col("ApplicationTypeID")
            .cast("int")
            .alias("ApplicationTypeKey"),

        col("ApplicationType")
            .alias("ApplicationTypeName")
    )
    .dropDuplicates(
        ["ApplicationTypeKey"]
    )
    .orderBy(
        "ApplicationTypeKey"
    )
)

# Display and validate the application-type dimension
display(df_stg_dim_application_type)

print(
    f"Application-type rows: "
    f"{df_stg_dim_application_type.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the populated application-status records
df_application_status_values = (
    df_gold
    .filter(
        col("ApplicationStatusID").isNotNull()
    )
    .select(
        col("ApplicationStatusID")
            .cast("int")
            .alias("ApplicationStatusKey"),

        col("ApplicationStatus")
            .alias("ApplicationStatusName")
    )
    .dropDuplicates(
        ["ApplicationStatusKey"]
    )
)

# Create the unknown application-status member
df_unknown_application_status = spark.createDataFrame(
    [(0, "Not recorded")],
    "ApplicationStatusKey INT, ApplicationStatusName STRING"
)

# Combine the source values with the unknown member
df_stg_dim_application_status = (
    df_unknown_application_status
    .unionByName(df_application_status_values)
    .orderBy("ApplicationStatusKey")
)

# Display and validate the application-status dimension
display(df_stg_dim_application_status)

print(
    f"Application-status rows: "
    f"{df_stg_dim_application_status.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the populated decision-type records
df_decision_type_values = (
    df_gold
    .filter(
        col("DecisionTypeID").isNotNull()
    )
    .select(
        col("DecisionTypeID")
            .cast("int")
            .alias("DecisionTypeKey"),

        col("DecisionType")
            .alias("DecisionTypeName"),

        col("DecisionOutcome")
    )
    .dropDuplicates(
        ["DecisionTypeKey"]
    )
)

# Create the unknown decision-type member
df_unknown_decision_type = spark.createDataFrame(
    [(0, "Not recorded", "No decision recorded")],
    """
    DecisionTypeKey INT,
    DecisionTypeName STRING,
    DecisionOutcome STRING
    """
)

# Combine the source decisions with the unknown member
df_stg_dim_decision_type = (
    df_unknown_decision_type
    .unionByName(df_decision_type_values)
    .orderBy("DecisionTypeKey")
)

# Display and validate the decision-type dimension
display(df_stg_dim_decision_type)

print(
    f"Decision-type rows: "
    f"{df_stg_dim_decision_type.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the populated land-use-type records
df_land_use_type_values = (
    df_gold
    .filter(
        col("LandUseTypeID").isNotNull()
    )
    .select(
        col("LandUseTypeID")
            .cast("int")
            .alias("LandUseTypeKey"),

        col("LandUseType")
            .alias("LandUseTypeName")
    )
    .dropDuplicates(
        ["LandUseTypeKey"]
    )
)

# Create the unknown land-use-type member
df_unknown_land_use_type = spark.createDataFrame(
    [(0, "Not recorded")],
    "LandUseTypeKey INT, LandUseTypeName STRING"
)

# Combine the source values with the unknown member
df_stg_dim_land_use_type = (
    df_unknown_land_use_type
    .unionByName(df_land_use_type_values)
    .orderBy("LandUseTypeKey")
)

# Display and validate the land-use-type dimension
display(df_stg_dim_land_use_type)

print(
    f"Land-use-type rows: "
    f"{df_stg_dim_land_use_type.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the planning-application fact staging dataset
df_stg_fact_planning_application = (
    df_gold
    .select(
        col("SourceObjectID")
            .cast("int")
            .alias("PlanningApplicationKey"),

        col("ApplicationNumber"),

        col("PlanningAuthorityID")
            .cast("int")
            .alias("PlanningAuthorityKey"),

        col("ApplicationTypeID")
            .cast("int")
            .alias("ApplicationTypeKey"),

        when(
            col("ApplicationStatusID").isNull(),
            lit(0)
        ).otherwise(
            col("ApplicationStatusID")
        ).cast("int").alias("ApplicationStatusKey"),

        when(
            col("DecisionTypeID").isNull(),
            lit(0)
        ).otherwise(
            col("DecisionTypeID")
        ).cast("int").alias("DecisionTypeKey"),

        when(
            col("LandUseTypeID").isNull(),
            lit(0)
        ).otherwise(
            col("LandUseTypeID")
        ).cast("int").alias("LandUseTypeKey"),

        date_format(
            col("ReceivedDate"),
            "yyyyMMdd"
        ).cast("int").alias("DateKey"),

        col("ReceivedDate"),
        col("DecisionDate"),
        col("GrantDate"),
        col("ExpiryDate"),

        lit(1)
            .cast("int")
            .alias("ApplicationCount"),

        col("DecisionRecorded")
            .cast("int"),

        when(
            col("OneOffHouse") == True,
            lit(1)
        ).when(
            col("OneOffHouse") == False,
            lit(0)
        ).otherwise(
            lit(None)
        ).cast("int").alias("OneOffHouseFlag"),

        col("NumResidentialUnits"),
        col("AreaOfSite"),
        col("FloorArea"),

        col("Latitude"),
        col("Longitude"),
        col("ITMEasting"),
        col("ITMNorthing"),

        substring(
            col("DevelopmentDescription"),
            1,
            4000
        ).alias("DevelopmentDescription"),
        col("DevelopmentAddress"),
        col("DevelopmentPostcode"),
        col("ApplicationDetailsURL"),

        col("SourceETLDate"),
        col("LoadDate"),
        col("GoldLoadTimestamp")
    )
)

# Display a sample of the planning fact
display(
    df_stg_fact_planning_application.limit(20)
)

# Verify the planning fact row count
print(
    f"Planning fact rows: "
    f"{df_stg_fact_planning_application.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the existing shared date staging dimension
df_stg_dim_date = spark.table(
    "stg_dim_date"
)

# Check for missing planning-authority keys
missing_authority_keys = (
    df_stg_fact_planning_application
    .select("PlanningAuthorityKey")
    .distinct()
    .join(
        df_stg_dim_planning_authority
            .select("PlanningAuthorityKey"),
        on="PlanningAuthorityKey",
        how="left_anti"
    )
    .count()
)

# Check for missing application-type keys
missing_application_type_keys = (
    df_stg_fact_planning_application
    .select("ApplicationTypeKey")
    .distinct()
    .join(
        df_stg_dim_application_type
            .select("ApplicationTypeKey"),
        on="ApplicationTypeKey",
        how="left_anti"
    )
    .count()
)

# Check for missing application-status keys
missing_application_status_keys = (
    df_stg_fact_planning_application
    .select("ApplicationStatusKey")
    .distinct()
    .join(
        df_stg_dim_application_status
            .select("ApplicationStatusKey"),
        on="ApplicationStatusKey",
        how="left_anti"
    )
    .count()
)

# Check for missing decision-type keys
missing_decision_type_keys = (
    df_stg_fact_planning_application
    .select("DecisionTypeKey")
    .distinct()
    .join(
        df_stg_dim_decision_type
            .select("DecisionTypeKey"),
        on="DecisionTypeKey",
        how="left_anti"
    )
    .count()
)

# Check for missing land-use-type keys
missing_land_use_type_keys = (
    df_stg_fact_planning_application
    .select("LandUseTypeKey")
    .distinct()
    .join(
        df_stg_dim_land_use_type
            .select("LandUseTypeKey"),
        on="LandUseTypeKey",
        how="left_anti"
    )
    .count()
)

# Check for dates missing from the shared date dimension
missing_date_keys = (
    df_stg_fact_planning_application
    .select("DateKey")
    .distinct()
    .join(
        df_stg_dim_date.select("DateKey"),
        on="DateKey",
        how="left_anti"
    )
    .count()
)

print(f"Missing planning-authority keys: {missing_authority_keys}")
print(f"Missing application-type keys: {missing_application_type_keys}")
print(f"Missing application-status keys: {missing_application_status_keys}")
print(f"Missing decision-type keys: {missing_decision_type_keys}")
print(f"Missing land-use-type keys: {missing_land_use_type_keys}")
print(f"Missing date keys: {missing_date_keys}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Identify planning dates missing from the shared date dimension
df_missing_planning_dates = (
    df_stg_fact_planning_application
    .select(
        "DateKey",
        "ReceivedDate"
    )
    .distinct()
    .join(
        df_stg_dim_date.select("DateKey"),
        on="DateKey",
        how="left_anti"
    )
    .orderBy("ReceivedDate")
)

# Display the missing planning dates
display(df_missing_planning_dates)

# Inspect the existing date-dimension schema
df_stg_dim_date.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import the required calendar functions
from pyspark.sql.functions import (
    year,
    quarter,
    month
)

# Generate one record for every calendar date from 2010 to 2030
df_calendar_dates = spark.sql(
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

# Create the complete shared date staging dimension
df_stg_dim_date = (
    df_calendar_dates
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

# Display and validate the complete date dimension
display(df_stg_dim_date.limit(20))

print(
    f"Complete date-dimension rows: "
    f"{df_stg_dim_date.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Recheck planning dates against the complete date dimension
missing_date_keys = (
    df_stg_fact_planning_application
    .select("DateKey")
    .distinct()
    .join(
        df_stg_dim_date.select("DateKey"),
        on="DateKey",
        how="left_anti"
    )
    .count()
)

print(f"Missing date keys after rebuild: {missing_date_keys}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the complete shared date staging dimension
df_stg_dim_date.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("stg_dim_date")

# Write the planning-authority staging dimension
df_stg_dim_planning_authority.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("stg_dim_planning_authority")

# Write the application-type staging dimension
df_stg_dim_application_type.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("stg_dim_application_type")

# Write the application-status staging dimension
df_stg_dim_application_status.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("stg_dim_application_status")

# Write the decision-type staging dimension
df_stg_dim_decision_type.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("stg_dim_decision_type")

# Write the land-use-type staging dimension
df_stg_dim_land_use_type.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("stg_dim_land_use_type")

# Write the planning-application staging fact
df_stg_fact_planning_application.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("stg_fact_planning_applications")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate the saved Warehouse staging-table counts
staging_tables = [
    "stg_dim_date",
    "stg_dim_planning_authority",
    "stg_dim_application_type",
    "stg_dim_application_status",
    "stg_dim_decision_type",
    "stg_dim_land_use_type",
    "stg_fact_planning_applications"
]

for table_name in staging_tables:
    table_count = spark.table(
        table_name
    ).count()

    print(
        f"{table_name}: "
        f"{table_count:,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
