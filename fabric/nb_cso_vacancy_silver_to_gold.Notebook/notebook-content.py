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

# Notebook: nb_cso_vacancy_silver_to_gold

# Transform structured local-authority vacancy data into county-level Gold housing metrics.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Imports - rename functions i.e. sum to spark_sum so s no confusion with Python’s functions
from pyspark.sql.functions import (
    col,
    when,
    lit,
    first,
    sum as spark_sum,
    max as spark_max,
    round as spark_round,
    regexp_extract,
    regexp_replace,
    make_date,
    current_timestamp
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Silver vacancy Delta dataset
df_silver = (
    spark.read
        .format("delta")
        .load("Files/Silver/cso_vacancy_local_authority")
)

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

# Pivot statistics into separate measure columns
df_local_authority = (
    df_silver

        .groupBy(
            "QuarterCode",
            "Quarter",
            "LocalAuthorityCode",
            "LocalAuthority",
            "SourceUpdatedTimestamp",
            "LoadDate"
        )

        .pivot(
            "StatisticCode",
            [
                "VAC14C01",
                "VAC14C02",
                "VAC14C03"
            ]
        )

        .agg(first("Value"))

        .withColumnRenamed(
            "VAC14C01",
            "VacantDwellings"
        )

        .withColumnRenamed(
            "VAC14C02",
            "DwellingStock"
        )

        .withColumnRenamed(
            "VAC14C03",
            "ReportedVacancyRate"
        )

        .withColumn(
            "VacantDwellings",
            col("VacantDwellings").cast("long")
        )

        .withColumn(
            "DwellingStock",
            col("DwellingStock").cast("long")
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate local-authority measures
print(
    f"Local-authority row count: "
    f"{df_local_authority.count()}"
)

display(
    df_local_authority
        .orderBy(
            "QuarterCode",
            "LocalAuthority"
        )
        .limit(31)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Map local authorities to geographic counties
df_county_mapped = (
    df_local_authority

        .withColumn(
            "County",

            when(
                col("LocalAuthority").isin(
                    "Dublin City Council",
                    "Dún Laoghaire Rathdown County Council",
                    "Fingal County Council",
                    "South Dublin County Council"
                ),
                lit("Dublin")
            )

            .when(
                col("LocalAuthority").isin(
                    "Cork City Council",
                    "Cork County Council"
                ),
                lit("Cork")
            )

            .when(
                col("LocalAuthority").isin(
                    "Galway City Council",
                    "Galway County Council"
                ),
                lit("Galway")
            )

            .when(
                col("LocalAuthority")
                == "Limerick City & County Council",
                lit("Limerick")
            )

            .when(
                col("LocalAuthority")
                == "Waterford City & County Council",
                lit("Waterford")
            )

            .otherwise(
                regexp_replace(
                    col("LocalAuthority"),
                    " County Council$",
                    ""
                )
            )
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate local-authority to county mapping
county_count = (
    df_county_mapped
        .select("County")
        .distinct()
        .count()
)

print(f"Distinct county count: {county_count}")

display(
    df_county_mapped
        .select(
            "LocalAuthority",
            "County"
        )
        .distinct()
        .orderBy(
            "County",
            "LocalAuthority"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Aggregate local-authority measures to county level
df_gold = (
    df_county_mapped

        .groupBy(
            "QuarterCode",
            "Quarter",
            "County"
        )

        .agg(
            spark_sum("VacantDwellings")
                .alias("VacantDwellings"),

            spark_sum("DwellingStock")
                .alias("DwellingStock"),

            spark_max("SourceUpdatedTimestamp")
                .alias("SourceUpdatedTimestamp"),

            spark_max("LoadDate")
                .alias("LoadDate")
        )

        .withColumn(
            "VacancyRate",
            spark_round(
                (
                    col("VacantDwellings") * lit(100.0)
                ) / col("DwellingStock"),
                1
            )
        )

        .withColumn(
            "Year",
            regexp_extract(
                col("Quarter"),
                r"^(\d{4})",
                1
            ).cast("int")
        )

        .withColumn(
            "QuarterNumber",
            regexp_extract(
                col("Quarter"),
                r"Q([1-4])$",
                1
            ).cast("int")
        )

        .withColumn(
            "QuarterStartDate",
            make_date(
                col("Year"),
                (
                    (col("QuarterNumber") - 1) * 3 + 1
                ).cast("int"),
                lit(1)
            )
        )

        .withColumn(
            "GoldLoadTimestamp",
            current_timestamp()
        )

        .select(
            "QuarterCode",
            "Quarter",
            "QuarterStartDate",
            "Year",
            "QuarterNumber",
            "County",
            "VacantDwellings",
            "DwellingStock",
            "VacancyRate",
            "SourceUpdatedTimestamp",
            "LoadDate",
            "GoldLoadTimestamp"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate county-level Gold dataset
print(f"Gold row count: {df_gold.count()}")

display(
    df_gold
        .orderBy(
            "QuarterCode",
            "County"
        )
        .limit(30)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save county vacancy metrics to Gold Delta
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Files/Gold/gold_housing_vacancy")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read and validate Gold Delta dataset
df_gold_check = (
    spark.read
        .format("delta")
        .load("Files/Gold/gold_housing_vacancy")
)

print(f"Gold Delta row count: {df_gold_check.count()}")

display(
    df_gold_check
        .orderBy(
            "QuarterCode",
            "County"
        )
        .limit(30)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
