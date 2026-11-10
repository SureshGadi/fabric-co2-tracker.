**🌍 Global CO2 Emissions Tracker:**

An end-to-end Microsoft Fabric project that analyzes global CO₂ emissions using Data Pipelines, Lakehouse, PySpark, Delta Tables, and Power BI.

**📌 Project Overview**:
This project uses the publicly available Our World in Data (OWID) CO₂ dataset to build a complete data engineering solution in Microsoft Fabric.

**The solution follows the Medallion Architecture approach:**

Bronze (Raw Data)
      ↓
Silver (Cleaned Data)
      ↓
Gold (Reporting Layer)
      ↓
Power BI Dashboard
🛠 Technologies Used
Microsoft Fabric
Data Pipelines
Lakehouse
PySpark
Delta Tables
SQL Analytics Endpoint
Semantic Model
Power BI
GitHub

**📊 Data Source**
Our World in Data (OWID) - CO₂ Dataset

Dataset: https://github.com/owid/co2-data

**📂 Architecture:**
OWID CSV
    ↓
Data Pipeline
    ↓
Bronze Layer
    ↓
Notebook 1
(Bronze → Silver)
    ↓
Silver Layer
    ↓
Notebook 2
(Silver → Gold)
    ↓
Gold Layer
    ↓
Semantic Model
    ↓
Power BI Dashboard

**📁 Data Layers:**
Bronze Layer
Stores raw CSV files exactly as received from the source.

Silver Layer
Cleans and transforms the data by:

Removing duplicates
Converting data types
Selecting required columns
Identifying countries and regions
Gold Layer

**Creates reporting-ready tables for Power BI analysis.**

**Tables created:**

silver_co2
dim_country
gold_country_year
gold_world_trend
gold_world_source_mix
gold_top10_latest

**📈 Dashboard Features**

Global View
World CO₂ Emissions
CO₂ Per Capita
Top Emitting Country
Emissions Trend Over Time
Emissions by Source
Top 10 Emitters
Country Explorer
Country Filter
Year Filter
Country Emission Trends
Country Rankings
Interactive Map

**🚀 Key Skills Demonstrated:**
Data Ingestion
PySpark Transformations
Delta Lake
Window Functions
Data Modeling
DAX Measures
Power BI Reporting
Pipeline Orchestration



**🔮 Future Enhancements:**
Incremental Loads
Data Quality Checks
Additional Data Sources
Automated Alerts
Fabric Warehouse Implementation

**👨‍💻 Author**
**Suresh Gadi**

Microsoft Fabric Data Engineering Project

GitHub: https://github.com/your-username
