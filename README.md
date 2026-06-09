
# CST8921 – Cloud Industry Trends 

## Lab 4: Azure Data Lake Storage Gen2 and Local Spark Data Processing

**Student:** Ilyas Zazai  
**Course:** CST8921 – Cloud Industry Trends  
**Lab Topic:** Azure Databricks / Spark analysis with Azure Data Lake Storage Gen2  
**Environment Used:** Azure Data Lake Storage Gen2 + Local PySpark in VS Code/WSL  

---

## 1. Introduction

In this lab, I worked with Azure Data Lake Storage Gen2 and Apache Spark to process e-commerce Parquet data. The original lab was designed to use Azure Databricks or Azure Synapse Analytics, but because there were subscription access issues with Databricks/Synapse, I used a local Spark environment in VS Code as the alternative method provided by the professor.

The main goal of the lab stayed the same: upload raw data to Azure storage, read the data using Spark, clean and transform the data, write the transformed data to a refined storage area, and analyze the final results.

This lab helped me understand a real cloud data engineering workflow:

```text
Raw data → Spark processing → Refined data → Analysis/report
```

---

## 2. Lab Objective

The objective of this lab was to:

- Upload Parquet files to Azure Data Lake Storage Gen2.
- Read raw Parquet data using Apache Spark.
- Clean and transform the data.
- Remove duplicate records.
- Fix timestamp data format issues.
- Add `Year` and `Month` columns for time-based analysis.
- Write transformed data back to Azure storage in a refined container.
- Analyze the refined data using Spark SQL.
- Create a simple visualization from the analysis result.

---

## 3. Original Lab Plan

The original lab expected students to use either:

```text
Azure Synapse Analytics
or
Azure Databricks
```

The expected flow was:

```text
Azure Data Lake Storage Gen2
   ↓
Azure Synapse / Azure Databricks
   ↓
Spark Notebook
   ↓
Transform raw data
   ↓
Write refined data
   ↓
Query and visualize results
```

However, I could not create or access Synapse/Databricks because of subscription permission issues. The error showed that I did not have permission to create a new Synapse workspace in the selected resource group.

---

## 4. What Changed

Because of the Azure subscription issue, the professor allowed students to use:

```text
Local Spark in VS Code
or
Docker image with Spark
```

So instead of running Spark inside Azure Synapse or Databricks, I ran Spark locally on my laptop using:

```text
WSL Ubuntu
VS Code
Python virtual environment
Java 17
PySpark
Azure Hadoop connector
```

The Azure storage part remained the same. The difference was only where Spark was running.

Original method:

```text
Spark runs in Azure Synapse/Databricks
```

My method:

```text
Spark runs locally on my laptop
```

The learning goal was still the same.

---

## 5. Final Architecture Used

```text
Azure Data Lake Storage Gen2
   |
   | raw container
   |-- customers/customers.parquet
   |-- orders/orders.parquet
   |-- order_events/order_events.parquet
   |
   v
Local Laptop / WSL / VS Code
   |
   | Java 17
   | Python virtual environment
   | PySpark
   | Azure Hadoop connector JARs
   |
   v
Local Spark Session
   |
   | Read raw Parquet from ADLS Gen2
   | Remove duplicates
   | Fix timestamp format
   | Add Year and Month columns
   |
   v
Azure Data Lake Storage Gen2
   |
   | refined container
   |-- order_events_refined/
       |-- Year=2023
       |-- Year=2024
       |-- Year=2025
       |-- Year=2026
       |-- _SUCCESS
```

---

## 6. Dataset Used

The dataset was an e-commerce synthetic dataset. It included three Parquet files:

| File | Purpose | Main Columns |
|---|---|---|
| `customers.parquet` | Customer profile data | `customer_id`, `country`, `signup_date` |
| `orders.parquet` | Order transaction data | `order_id`, `customer_id`, `order_date`, `order_amount`, `order_status` |
| `order_events.parquet` | Order event data | `event_id`, `order_id`, `event_time`, `event_type` |

For the main transformation, I used:

```text
order_events.parquet
```

This file was used because the lab transformation steps focused on the `event_time` column.

---

## 7. Azure Storage Setup

I created an Azure Storage Account with Hierarchical Namespace enabled. This made the storage account work as Azure Data Lake Storage Gen2.

I created two containers:

```text
raw
refined
```

The raw container stored the original files:

```text
raw/
 ├── customers/
 │    └── customers.parquet
 ├── orders/
 │    └── orders.parquet
 └── order_events/
      └── order_events.parquet
```

The refined container was used later to store the transformed Spark output.

---

## 8. Local Spark Setup

To run Spark locally, I installed Java and PySpark.

### Software Installed

```text
Java 17
Python virtual environment
PySpark
pandas
matplotlib
pyarrow
python-dotenv
```

### Why Java Was Needed

Spark runs on the JVM, so Java was required before PySpark could start a Spark session.

### Why PySpark Was Used

PySpark allowed me to write Spark code using Python. It was used to read Parquet files, transform data, write refined output, and run SQL-style analysis.

### Why Azure Hadoop Connector Was Used

Local Spark does not automatically understand Azure Storage paths. The Azure Hadoop connector allowed Spark to read from and write to Azure Data Lake Storage Gen2.

The connector packages were:

```text
org.apache.hadoop:hadoop-azure
com.microsoft.azure:azure-storage
```

---

## 9. Why `abfss://` Was Used

Because the storage account had Hierarchical Namespace enabled, it was treated as ADLS Gen2.

For ADLS Gen2, the correct secure path format is:

```text
abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
```

Example:

```text
abfss://raw@cst8921lab4ilyasxxxx.dfs.core.windows.net/order_events/order_events.parquet
```

This means:

| Part | Meaning |
|---|---|
| `abfss://` | Secure ADLS Gen2 file system protocol |
| `raw` | Container name |
| `storage-account` | Azure Storage Account name |
| `dfs.core.windows.net` | ADLS Gen2 endpoint |
| `order_events/order_events.parquet` | File path |

---

## 10. Reading Raw Data with Spark

I created a PySpark script to connect to Azure Data Lake Storage Gen2 and read the `order_events.parquet` file.

The raw file contained:

```text
422,100 rows
```

The schema included:

```text
event_id
order_id
event_time
event_type
```

---

## 11. Problem Faced: Parquet Timestamp Error

When I first tried to read the Parquet file, Spark showed this error:

```text
PARQUET_TYPE_ILLEGAL
Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))
```

This happened because the `event_time` column was stored in nanosecond timestamp format. Spark did not read this format correctly by default.

Before fixing, the timestamp looked like a very large number:

```text
1683679528000000000
```

After fixing, it became a readable timestamp:

```text
2023-05-09 20:45:28
```

This was an important troubleshooting step because real cloud data work often includes data type and compatibility issues.

---

## 12. Data Transformation Steps

### Step 1: Read Raw Data

I read the raw event data from:

```text
raw/order_events/order_events.parquet
```

### Step 2: Remove Duplicates

The dataset included duplicate event records, so I removed duplicates using Spark:

```python
df_dedup = df_raw.dropDuplicates()
```

This cleaned the dataset before analysis.

### Step 3: Fix Timestamp Data Type

I fixed the `event_time` column and converted it into a proper timestamp format.

This was required because the next step needed to extract the year and month from `event_time`.

### Step 4: Add Year and Month Columns

I created two new columns:

```text
Year
Month
```

Example:

```text
event_time = 2025-10-31 21:58:03
Year = 2025
Month = 10
```

This made the data easier to analyze by time period.

### Step 5: Write to Refined Container

I wrote the transformed data back to Azure Data Lake Storage Gen2 in the refined container:

```text
refined/order_events_refined/
```

I also partitioned the output by:

```text
Year
Month
```

The refined container showed:

```text
Year=2023
Year=2024
Year=2025
Year=2026
_SUCCESS
```

---

## 13. Why Partitioning Was Important

Partitioning organizes data into folders based on column values.

Instead of saving all transformed data in one folder, Spark created folders like:

```text
order_events_refined/
 ├── Year=2023/
 ├── Year=2024/
 ├── Year=2025/
 └── Year=2026/
```

This is useful because if a query only needs 2025 data, the system can read only the 2025 folder instead of scanning all data.

Partitioning helps with:

- Better data organization.
- Faster queries.
- Lower processing cost in large data systems.
- Cleaner data lake structure.

The `_SUCCESS` file means Spark completed the write operation successfully.

---

## 14. Analysis Results

After writing the refined data, I read it again and created a Spark SQL temporary view called:

```text
refined_events
```

Then I used Spark SQL to analyze the data.

### 14.1 Total Events by Year

| Year | Total Events |
|---|---:|
| 2023 | 138,601 |
| 2024 | 140,316 |
| 2025 | 139,870 |
| 2026 | 1,213 |

Most events happened between 2023 and 2025. A small number of events appeared in 2026 because some orders from late 2025 had events a few days later.

### 14.2 Event Counts by Type

| Event Type | Total Events |
|---|---:|
| cancelled | 20,931 |
| delivered | 84,129 |
| order_created | 50,479 |
| packed | 58,888 |
| payment_authorized | 50,472 |
| payment_captured | 75,669 |
| returned | 12,552 |
| shipped | 66,880 |

The highest event type was:

```text
delivered = 84,129
```

This was followed by:

```text
payment_captured = 75,669
shipped = 66,880
packed = 58,888
```

### 14.3 Events by Year and Event Type

| Year | Event Type | Total Events |
|---|---|---:|
| 2023 | cancelled | 6,935 |
| 2023 | delivered | 27,825 |
| 2023 | order_created | 16,611 |
| 2023 | packed | 19,565 |
| 2023 | payment_authorized | 16,695 |
| 2023 | payment_captured | 25,011 |
| 2023 | returned | 4,119 |
| 2023 | shipped | 21,840 |
| 2024 | cancelled | 7,021 |
| 2024 | delivered | 28,088 |
| 2024 | order_created | 16,992 |
| 2024 | packed | 19,466 |
| 2024 | payment_authorized | 16,834 |
| 2024 | payment_captured | 25,324 |
| 2024 | returned | 4,271 |
| 2024 | shipped | 22,320 |
| 2025 | cancelled | 6,918 |
| 2025 | delivered | 27,968 |
| 2025 | order_created | 16,728 |
| 2025 | packed | 19,669 |
| 2025 | payment_authorized | 16,807 |
| 2025 | payment_captured | 25,109 |
| 2025 | returned | 4,135 |
| 2025 | shipped | 22,536 |
| 2026 | cancelled | 57 |
| 2026 | delivered | 248 |
| 2026 | order_created | 148 |
| 2026 | packed | 188 |
| 2026 | payment_authorized | 136 |
| 2026 | payment_captured | 225 |
| 2026 | returned | 27 |
| 2026 | shipped | 184 |

---

## 15. Interpretation of Results

The event type results show a normal e-commerce order lifecycle:

```text
order_created
   ↓
payment_authorized
   ↓
payment_captured
   ↓
packed
   ↓
shipped
   ↓
delivered
```

Some orders were cancelled or returned, which is also normal in an e-commerce system.

The yearly results were very close for 2023, 2024, and 2025, which means the dataset was distributed across those years. The small 2026 count happened because event times can occur after the order date, especially for orders placed at the end of 2025.

---

## 16. Visualization

I created a bar chart using Python `matplotlib`.

The chart showed:

```text
Total Order Events by Year
```

The chart was saved as:

```text
events_by_year.png
```

This visualization helped show that most events happened in 2023, 2024, and 2025, with a small number in 2026.

---

## 17. Screenshots Captured

The important screenshots for this lab were:

1. Raw container showing `customers`, `orders`, and `order_events` folders.
2. Local Spark reading raw `order_events.parquet` from ADLS Gen2.
3. Raw schema and first five rows.
4. Raw row count.
5. Timestamp conversion result.
6. Transformed schema with `Year` and `Month` columns.
7. Refined container showing `order_events_refined`.
8. Partition folders: `Year=2023`, `Year=2024`, `Year=2025`, `Year=2026`.
9. SQL-style analysis showing total events by year.
10. Event counts by event type.
11. Bar chart showing events by year.

---

## 18. Cleanup

After completing the lab and saving the report evidence, I cleaned up the resources.

### Azure Cleanup

I deleted the Azure resource group to remove the storage account and related resources:

```bash
az group delete --name rg-cst8921-lab4-datalake --yes --no-wait
```

### Local Secret Cleanup

I removed the `.env` file because it contained the Azure Storage account key:

```bash
rm -f .env
```

### Local Python Environment Cleanup

I removed the virtual environment:

```bash
rm -rf .venv
```

### Spark Connector Cache Cleanup

I removed the downloaded Spark connector cache:

```bash
rm -rf ~/.ivy2.5.2
```

Java 17 can be kept if it is needed for future Java, Spring Boot, or Spark labs.

---

## 19. Reflection

This lab helped me understand how a real data lake workflow works. I learned that raw files are stored first in a data lake, then Spark is used to clean and transform the data, and finally the refined data is saved for analysis.

I also learned that Spark can run in different environments. It can run in Azure Databricks, Azure Synapse, Docker, or locally on a laptop. In this lab, Synapse and Databricks were not available because of subscription permission issues, so I used local Spark in VS Code. This helped me understand that the main Spark processing concept is the same even when the environment changes.

One important issue I faced was the Parquet timestamp format error. The `event_time` column was stored in nanosecond format, and Spark did not read it correctly at first. I fixed this problem by changing the Spark configuration and converting the timestamp into a readable format. This was useful because real cloud and data engineering work often includes troubleshooting data format issues.

I also learned the importance of separating data into raw and refined zones. The raw zone keeps the original files unchanged, while the refined zone stores cleaned and transformed data. This is a common data lake design pattern in cloud analytics.

---

## 20. Conclusion

In conclusion, this lab showed how Azure Data Lake Storage Gen2 and Apache Spark can be used together to process and analyze Parquet data. I uploaded raw e-commerce data to Azure storage, connected local PySpark to the storage account, read the Parquet file, cleaned the data, fixed the timestamp issue, added `Year` and `Month` columns, and saved the transformed output to the refined container.

The final analysis showed event counts by year and event type. The results helped explain the order event lifecycle in the dataset. Even though I used local Spark instead of Synapse or Databricks, the main cloud data processing workflow remained the same.

The most important learning from this lab was:

```text
Raw data is stored in the data lake.
Spark processes and transforms the data.
Refined data is saved back to storage.
The refined data is then used for analysis and reporting.
```

This lab gave me practical experience with Azure Data Lake Storage Gen2, Spark, PySpark, Parquet files, data transformation, partitioning, troubleshooting, and cloud analytics workflow.

---

## 21. Simple Summary for Review

```text
We uploaded raw e-commerce event data to Azure Data Lake.
Because Synapse/Databricks was blocked, we used local Spark in VS Code.
Spark read the raw Parquet data from Azure.
We cleaned the data by removing duplicates and fixing timestamp format.
We added Year and Month columns for time-based analysis.
We wrote the transformed data back to Azure in the refined container.
Then we analyzed the refined data by year and event type.
```
