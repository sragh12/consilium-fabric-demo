# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9f0bf933-5bc2-4c2a-8f69-161bf930b7b3",
# META       "default_lakehouse_name": "EUCitizensLakehouse",
# META       "default_lakehouse_workspace_id": "430b6bd7-2116-42be-9e4b-ef052e523d1c",
# META       "known_lakehouses": [
# META         {
# META           "id": "9f0bf933-5bc2-4c2a-8f69-161bf930b7b3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # EU Citizens Dashboard - Fabric Lakehouse setup
# Synthetic demo data only. Attach this notebook to a Fabric Lakehouse before running.


# CELL ********************

df = spark.sql("SELECT * FROM EUCitizensLakehouse.dbo.dim_country LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Initialize notebook
# This cell imports PySpark helper libraries, brings in Fabric notebook utilities, and sets the base folder path in the Lakehouse (`Files/eu_citizens_demo`) where the CSV demo files are stored.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *
from notebookutils import mssparkutils

base_path = "Files/eu_citizens_demo"
print(f"Using Lakehouse path: {base_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Upload files
# Upload the four CSV files supplied with this kit to **Files/eu_citizens_demo/** in the attached Lakehouse.


# MARKDOWN ********************

# ## Load source CSV files
# This cell reads the four input CSV files from the Lakehouse (`dim_country`, `dim_indicator`, `fact_indicator`, and `translations`) into Spark DataFrames using header and schema inference, then displays samples for a quick visual data check.

# CELL ********************

country = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/dim_country.csv")
indicator = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/dim_indicator.csv")
fact = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/fact_indicator.csv")
translations = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/translations.csv")

display(country.limit(5))
display(indicator)
display(fact.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run basic data quality checks
# This cell validates the integrity of the loaded data by checking for:
# - Unique country and indicator keys
# - Missing values in the `Value` column of the fact data
# - Orphaned foreign keys in the fact table that do not exist in the dimension tables
# If any check fails, the notebook will raise an assertion error.

# CELL ********************

# Basic quality checks
assert country.select("CountryCode").distinct().count() == country.count(), "Duplicate country keys"
assert indicator.select("IndicatorCode").distinct().count() == indicator.count(), "Duplicate indicator keys"
assert fact.filter(F.col("Value").isNull()).count() == 0, "Null values found"
assert fact.join(country, "CountryCode", "left_anti").count() == 0, "Unknown country key"
assert fact.join(indicator, "IndicatorCode", "left_anti").count() == 0, "Unknown indicator key"
print("Quality checks passed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Clean fact data and create Delta tables
# This cell:
# - Cleans the `fact_indicator` DataFrame by casting `AsOfDate`, `Year`, and `Value` to proper types
# - Writes the dimension and fact DataFrames as Delta tables in the Lakehouse (`dim_country`, `dim_indicator`, `fact_indicator`, `dim_translation`), overwriting any existing tables with the same names.

# CELL ********************

# Add a proper date and load Delta tables
fact_clean = (fact
    .withColumn("AsOfDate", F.to_date("AsOfDate"))
    .withColumn("Year", F.col("Year").cast("int"))
    .withColumn("Value", F.col("Value").cast("double")))

country.write.mode("overwrite").format("delta").saveAsTable("dim_country")
indicator.write.mode("overwrite").format("delta").saveAsTable("dim_indicator")
fact_clean.write.mode("overwrite").format("delta").saveAsTable("fact_indicator")
translations.write.mode("overwrite").format("delta").saveAsTable("dim_translation")
print("Delta tables created")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create denormalized Gold view
# This cell builds or replaces the `vw_indicator_gold` view, joining the fact and dimension tables into a single, report-friendly dataset that includes country attributes, indicator metadata, and the numeric values ready for BI consumption.

# CELL ********************

# Create a denormalized Gold view 
spark.sql("""
CREATE OR REPLACE VIEW vw_indicator_gold AS
SELECT
  f.CountryCode,
  c.CountryName_EN, c.CountryName_FR, c.CountryName_NL,
  c.Region, c.PopulationThousands,
  f.Year, f.AsOfDate,
  f.IndicatorCode,
  i.IndicatorName_EN, i.IndicatorName_FR, i.IndicatorName_NL,
  i.Category, i.Unit, i.Direction,
  f.Value, f.SourceLabel
FROM fact_indicator f
JOIN dim_country c ON f.CountryCode = c.CountryCode
JOIN dim_indicator i ON f.IndicatorCode = i.IndicatorCode
""")
display(spark.sql("SELECT * FROM vw_indicator_gold LIMIT 20"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Build validation aggregate
# This cell creates a simple aggregate query that calculates the EU-wide average value per `IndicatorCode` and `Year`. It is intended for quick validation and comparison during the demo.

# CELL ********************

# Optional aggregate used for instant validation in the demo
validation = spark.sql("""
SELECT IndicatorCode, Year, ROUND(AVG(Value), 2) AS EUAverage
FROM fact_indicator
GROUP BY IndicatorCode, Year
ORDER BY IndicatorCode, Year
""")
display(validation)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
