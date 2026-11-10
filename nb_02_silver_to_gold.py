#!/usr/bin/env python
# coding: utf-8

# ## nb_02_silver_to_gold
# 
# null

# In[1]:


from pyspark.sql import functions as F, Window
s = spark.table("silver_co2")


# In[2]:


w_country = Window.partitionBy("country").orderBy("year")
w_year = Window.partitionBy("year").orderBy(F.col("co2").desc_nulls_last())
fact = (s.filter("is_country")
.withColumn("co2_prev", F.lag("co2").over(w_country))
.withColumn("co2_yoy_pct", F.round(
(F.col("co2") - F.col("co2_prev")) / F.col("co2_prev") * 100, 2))
.withColumn("rank_in_year", F.dense_rank().over(w_year))
.drop("co2_prev", "is_country"))
fact.write.mode("overwrite").format("delta").saveAsTable("gold_country_year")


# In[3]:


(s.filter("is_country").select("country", "iso_code").distinct()
.write.mode("overwrite").format("delta").saveAsTable("dim_country"))
world = (s.filter("country = 'World'")
.select("year", "co2", "co2_per_capita", "population", "cumulative_co2"))
world.write.mode("overwrite").format("delta").saveAsTable("gold_world_trend")


# In[4]:


mix = (s.filter("country = 'World'")
.select("year", F.expr("""stack(5,
'Coal', coal_co2, 'Oil', oil_co2, 'Gas', gas_co2,
'Cement', cement_co2, 'Flaring', flaring_co2) AS (source, co2)"""))
.filter("co2 IS NOT NULL"))
mix.write.mode("overwrite").format("delta").saveAsTable("gold_world_source_mix")


# In[5]:


latest = fact.filter("co2 IS NOT NULL").agg(F.max("year")).first()[0]
print("latest year with data:", latest)
top10 = (fact.filter(F.col("year") == latest)
.orderBy(F.col("co2").desc()).limit(10)
.select("country", "year", "co2", "co2_per_capita", "share_global_co2"))
top10.write.mode("overwrite").format("delta").saveAsTable("gold_top10_latest")
top10.show()

