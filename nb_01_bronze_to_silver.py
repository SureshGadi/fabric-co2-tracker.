#!/usr/bin/env python
# coding: utf-8

# ## nb_01_bronze_to_silver
# 
# null

# In[ ]:


from pyspark.sql import functions as F
raw = (spark.read.option("header", True)
.option("inferSchema", True)
.csv("Files/bronze/co2/owid-co2-data.csv"))
print("rows:", raw.count(), "| columns:", len(raw.columns))
raw.printSchema()


# In[2]:


keep = ["country", "year", "iso_code", "population", "gdp", "co2",
"co2_per_capita", "cumulative_co2", "coal_co2", "oil_co2",
"gas_co2", "cement_co2", "flaring_co2", "share_global_co2"]
keep = [c for c in keep if c in raw.columns] # safe if OWID renames a column
df = (raw.select(*keep)
.withColumn("country", F.trim("country"))
.withColumn("year", F.col("year").cast("int"))
.filter(F.col("country").isNotNull() & F.col("year").isNotNull())
.dropDuplicates(["country", "year"]))


# In[3]:


# Regions such as World, Asia, Europe are not countries, so flag them.
# Kosovo uses the code OWID_KOS but is treated as a country here.
df = df.withColumn(
    "is_country",
    F.when(F.col("iso_code").isNull(), False)
    .when(F.col("iso_code").startswith("OWID_") & (F.col("iso_code") != "OWID_KOS"), False)
    .otherwise(True))


# In[ ]:


print("negative co2 rows:", df.filter(F.col("co2") < 0).count())
print("country rows:", df.filter("is_country").count())
df.write.mode("overwrite").format("delta").saveAsTable("silver_co2")
spark.sql("""SELECT country, year, co2 FROM silver_co2 
              WHERE country = 'India' ORDER BY year DESC LIMIT 5""").show()


# In[1]:


spark.stop()

