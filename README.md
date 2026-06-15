# Spark SQL – Bike Sharing System Analysis

## Overview

This practical activity demonstrates the use of Apache Spark SQL for analyzing a public bike-sharing system dataset.

The objective is to extract valuable insights regarding:

- User behavior
- Station popularity
- Peak usage hours
- Rental trends
- Overall system performance

The application is developed in **Java** using **Apache Spark SQL** and executes a series of analytical queries on a CSV dataset containing bike rental transactions.

---

## Dataset Description

Dataset file:

```text
bike_sharing.csv
```

### Dataset Structure

| Column | Description |
|----------|-------------|
| rental_id | Unique identifier of a rental |
| user_id | Unique identifier of a user |
| age | User age |
| gender | User gender (M/F) |
| start_time | Rental start timestamp |
| end_time | Rental end timestamp |
| start_station | Station where the bike was picked up |
| end_station | Station where the bike was returned |
| duration_minutes | Duration of the rental in minutes |
| price | Rental cost in dollars |

---

## Technologies Used

- Java 21
- Apache Spark Core 4.1.1
- Apache Spark SQL 4.1.1
- Maven

---

## Project Structure

```text
spark-sql-tp/
│
├── pom.xml
├── README.md
│
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── enset/
│       │           └── bigdata/
│       │               └── sparksql/
│       │                   └── SparkSqlTpApplication.java
│       │
│       └── resources/
│           ├── bike_sharing.csv
│           └── application.properties
│
└── target/
```

---

# Exercise 1 – Data Loading & Exploration

## Objectives

1. Load the CSV file into a Spark DataFrame.
2. Display the DataFrame schema.
3. Show the first five records.
4. Count the total number of rentals.

### Implemented Operations

- CSV loading using Spark DataFrame API
- Automatic schema inference
- Data preview
- Record counting

### Spark Code

```java
Dataset<Row> df = spark.read()
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvPath);

df.printSchema();

df.show(5, false);

long totalRentals = df.count();
```

---

# Exercise 2 – Create a Temporary SQL View

## Objective

Create a Spark SQL temporary view named:

```sql
bike_rentals_view
```

### Spark Code

```java
df.createOrReplaceTempView("bike_rentals_view");
```

---

# Exercise 3 – Basic SQL Queries

## 3.1 Rentals Longer Than 30 Minutes

```sql
SELECT rental_id,
       user_id,
       start_station,
       end_station,
       duration_minutes
FROM bike_rentals_view
WHERE duration_minutes > 30
ORDER BY duration_minutes DESC;
```

---

## 3.2 Rentals Starting at Station A

```sql
SELECT rental_id,
       user_id,
       start_time,
       end_station,
       duration_minutes
FROM bike_rentals_view
WHERE start_station = 'Station A'
ORDER BY start_time;
```

---

## 3.3 Total Revenue

```sql
SELECT ROUND(SUM(price), 2) AS total_revenue
FROM bike_rentals_view;
```

---

# Exercise 4 – Aggregation Queries

## 4.1 Number of Rentals per Start Station

```sql
SELECT start_station,
       COUNT(*) AS nb_rentals
FROM bike_rentals_view
GROUP BY start_station
ORDER BY nb_rentals DESC;
```

---

## 4.2 Average Rental Duration per Start Station

```sql
SELECT start_station,
       ROUND(AVG(duration_minutes), 2) AS avg_duration_min
FROM bike_rentals_view
GROUP BY start_station
ORDER BY avg_duration_min DESC;
```

---

## 4.3 Most Popular Station

```sql
SELECT start_station,
       COUNT(*) AS nb_rentals
FROM bike_rentals_view
GROUP BY start_station
ORDER BY nb_rentals DESC
LIMIT 1;
```

---

# Exercise 5 – Time-Based Analysis

## 5.1 Extract Rental Hour

```sql
SELECT rental_id,
       start_time,
       HOUR(start_time) AS hour_of_day
FROM bike_rentals_view;
```

---

## 5.2 Rentals per Hour

```sql
SELECT HOUR(start_time) AS hour_of_day,
       COUNT(*) AS nb_rentals
FROM bike_rentals_view
GROUP BY HOUR(start_time)
ORDER BY nb_rentals DESC;
```

### Objective

Identify the peak rental hours during the day.

---

## 5.3 Most Popular Morning Station

Morning period: 07:00 – 12:00

```sql
SELECT start_station,
       COUNT(*) AS nb_rentals
FROM bike_rentals_view
WHERE HOUR(start_time) BETWEEN 7 AND 11
GROUP BY start_station
ORDER BY nb_rentals DESC
LIMIT 1;
```

---

# Exercise 6 – User Behavior Analysis

## 6.1 Average User Age

```sql
SELECT ROUND(AVG(age), 2) AS avg_age
FROM bike_rentals_view;
```

---

## 6.2 Rentals by Gender

```sql
SELECT gender,
       COUNT(*) AS nb_rentals
FROM bike_rentals_view
GROUP BY gender
ORDER BY nb_rentals DESC;
```

---

## 6.3 Most Active Age Group

Age groups:

- 18–30
- 31–40
- 41–50
- 51+

```sql
SELECT
    CASE
        WHEN age BETWEEN 18 AND 30 THEN '18-30'
        WHEN age BETWEEN 31 AND 40 THEN '31-40'
        WHEN age BETWEEN 41 AND 50 THEN '41-50'
        ELSE '51+'
    END AS age_group,
    COUNT(*) AS nb_rentals
FROM bike_rentals_view
GROUP BY age_group
ORDER BY nb_rentals DESC;
```
