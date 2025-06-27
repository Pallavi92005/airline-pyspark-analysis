✈️ Airline Data Analysis with PySpark
This project uses PySpark to perform scalable and efficient analysis on large airline datasets. The goal is to derive insights such as flight delays, cancellations, busiest routes, and performance trends from real-world flight data using distributed computing.


# ✈️ Airline Data Analysis with PySpark

This project explores and analyzes large-scale airline datasets using **Apache Spark (PySpark)**. With the power of distributed processing, the analysis is optimized for handling millions of rows efficiently, making it ideal for real-world big data scenarios.

## 🔍 Objectives

- Analyze flight delay patterns (arrival and departure)
- Identify most common reasons for flight cancellations
- Determine busiest airports and flight routes
- Perform monthly and yearly aggregations
- Handle missing/null values in large datasets
- Use Spark SQL for complex querying and filtering

## 🧠 Technologies Used

- Apache Spark (PySpark)
- Python
- Jupyter Notebook
- Spark DataFrames & Spark SQL
- Optional: Matplotlib/Seaborn for visualization (if exporting to Pandas)

## 📊 Dataset Overview

The dataset typically includes the following fields:

- `FlightDate`, `Airline`, `FlightNum`, `Origin`, `Dest`
- `DepDelay`, `ArrDelay`, `Cancelled`, `CancellationCode`
- `Distance`, `Year`, `Month`, `DayOfWeek`

Data Source: [U.S. Department of Transportation](https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236)

## ⚙️ Key PySpark Operations

- DataFrame creation and schema inference
- GroupBy, Aggregations, and Joins
- Filtering and Conditional Logic
- SQL Queries with `spark.sql()`
- Handling null values and duplicates

