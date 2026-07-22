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

# Notebook: nb_planning_silver_to_gold

# Transform cleaned Silver planning-application data into Gold business analytics datasets.

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
    current_timestamp,
    year,
    month,
    date_format,
    upper
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the Silver planning-applications Delta table
df_silver = spark.read.format("delta").load(
    "Files/Silver/planning_applications"
)

# Display a sample of the Silver data
display(df_silver.limit(10))

# Verify the Silver row count
print(f"Silver row count: {df_silver.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the canonical Gold planning-applications dataset
df_gold_planning_applications = (
    df_silver
    .withColumn(
        "ApplicationYear",
        year(col("ReceivedDate"))
    )
    .withColumn(
        "ApplicationMonth",
        month(col("ReceivedDate"))
    )
    .withColumn(
        "ApplicationYearMonth",
        date_format(col("ReceivedDate"), "yyyy-MM")
    )
    .withColumn(
        "OneOffHouseCategory",
        when(
            col("OneOffHouse") == True,
            lit("Yes")
        ).when(
            col("OneOffHouse") == False,
            lit("No")
        ).otherwise(
            lit("Not recorded")
        )
    )
    .withColumn(
        "DecisionOutcome",
        when(
            upper(col("DecisionType")).contains("REFUS"),
            lit("Refused")
        ).when(
            upper(col("DecisionType")).contains("GRANT") |
            upper(col("DecisionType")).contains("CONDITIONAL") |
            upper(col("DecisionType")).contains("APPROV"),
            lit("Granted")
        ).when(
            upper(col("DecisionType")).contains("EXEMPT"),
            lit("Exempt")
        ).when(
            col("DecisionType") == "Not recorded",
            lit("No decision recorded")
        ).otherwise(
            lit("Other")
        )
    )
    .withColumn(
        "DecisionRecorded",
        when(
            col("DecisionType") == "Not recorded",
            lit(0)
        ).otherwise(
            lit(1)
        )
    )
    .withColumn(
        "GoldLoadTimestamp",
        current_timestamp()
    )
)

# Display the new Gold business fields
display(
    df_gold_planning_applications.select(
        "SourceObjectID",
        "ApplicationNumber",
        "PlanningAuthority",
        "ReceivedDate",
        "ApplicationYear",
        "ApplicationMonth",
        "ApplicationYearMonth",
        "DecisionType",
        "DecisionOutcome",
        "OneOffHouseCategory",
        "Latitude",
        "Longitude"
    ).limit(20)
)

# Verify that the Gold transformation retained every application
print(
    f"Gold planning row count: "
    f"{df_gold_planning_applications.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Review the standardised decision-outcome categories
df_decision_outcome_check = (
    df_gold_planning_applications
    .groupBy("DecisionOutcome")
    .agg(
        count("*").alias("ApplicationCount")
    )
    .orderBy(
        col("ApplicationCount").desc()
    )
)

display(df_decision_outcome_check)

# Review the standardised one-off-house categories
df_one_off_house_check = (
    df_gold_planning_applications
    .groupBy("OneOffHouseCategory")
    .agg(
        count("*").alias("ApplicationCount")
    )
    .orderBy(
        col("ApplicationCount").desc()
    )
)

display(df_one_off_house_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect the original decision values currently classified as Other
df_other_decisions = (
    df_gold_planning_applications
    .filter(
        col("DecisionOutcome") == "Other"
    )
    .groupBy("DecisionType")
    .agg(
        count("*").alias("ApplicationCount")
    )
    .orderBy(
        col("ApplicationCount").desc()
    )
)

display(df_other_decisions.limit(30))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a reusable uppercase decision expression
decision_text = upper(col("DecisionType"))

# Refine the standardised decision-outcome categories
df_gold_planning_applications = (
    df_gold_planning_applications
    .withColumn(
        "DecisionOutcome",
        when(
            col("DecisionType").isin(
                "Not recorded",
                "N/A"
            ),
            lit("No decision recorded")
        ).when(
            decision_text.contains("INVA"),
            lit("Invalid")
        ).when(
            decision_text.contains("WITHDRAW"),
            lit("Withdrawn")
        ).when(
            decision_text.contains("SPLIT"),
            lit("Split decision")
        ).when(
            decision_text.contains("REFUS"),
            lit("Refused")
        ).when(
            decision_text.contains("GRANT") |
            decision_text.contains("CONDITIONAL") |
            decision_text.contains("APPROV"),
            lit("Granted")
        ).when(
            decision_text.contains("EXEMPT"),
            lit("Exempt")
        ).when(
            decision_text.contains("ADDITIONAL INFORMATION") |
            decision_text.contains("FURTHER INFORMATION") |
            decision_text.contains("REQUEST AI") |
            decision_text.contains("REQ AI"),
            lit("Further information")
        ).otherwise(
            lit("Other")
        )
    )
)

# Review the refined decision categories
display(
    df_gold_planning_applications
    .groupBy("DecisionOutcome")
    .agg(
        count("*").alias("ApplicationCount")
    )
    .orderBy(
        col("ApplicationCount").desc()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Align the decision-recorded indicator with the refined outcome
df_gold_planning_applications = (
    df_gold_planning_applications
    .withColumn(
        "DecisionRecorded",
        when(
            col("DecisionOutcome") == "No decision recorded",
            lit(0)
        ).otherwise(
            lit(1)
        )
    )
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the canonical planning dataset to the Gold Delta layer
df_gold_planning_applications.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .save("Files/Gold/gold_planning_applications")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the Gold planning-applications Delta table
df_gold_check = spark.read.format("delta").load(
    "Files/Gold/gold_planning_applications"
)

# Display a sample of the saved Gold data
display(
    df_gold_check.select(
        "SourceObjectID",
        "ApplicationNumber",
        "PlanningAuthority",
        "ReceivedDate",
        "ApplicationYear",
        "DecisionType",
        "DecisionOutcome",
        "OneOffHouseCategory",
        "Latitude",
        "Longitude",
        "LoadDate",
        "GoldLoadTimestamp"
    ).limit(20)
)

# Verify the saved Gold row count
print(f"Saved Gold row count: {df_gold_check.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
