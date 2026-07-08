# ✈️ US Flight Delay Analytics Dashboard

## 📌 Project Overview

The **US Flight Delay Analytics Dashboard** is an end-to-end data engineering and analytics project that processes historical US domestic flight data to generate actionable insights into airline performance, delays, cancellations, and airport operations.

The project demonstrates a modern data engineering workflow using **Apache Spark (PySpark)**, **Delta Lake**, and **Power BI**, showcasing scalable data transformation and interactive business intelligence reporting.

---

## 🎯 Objectives

- Analyze flight performance across US airlines.
- Identify the major causes of flight delays.
- Measure cancellation trends.
- Compare airline punctuality.
- Analyze airport-wise operational efficiency.
- Build an optimized analytics dataset for reporting.

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|------------|
| Programming | Python |
| Data Processing | Apache Spark (PySpark) |
| Data Storage | Delta Lake |
| Data Platform | Databricks Community Edition |
| Visualization | Power BI |
| Data Source | US Bureau of Transportation Statistics (BTS) |

---

## 📂 Project Architecture

```
US Flight Dataset (CSV)
           │
           ▼
PySpark Data Ingestion
           │
           ▼
Data Cleaning
           │
           ▼
Feature Engineering
           │
           ▼
Delta Lake Tables
           │
           ▼
Power BI Dashboard
```

---

## 📁 Project Structure

```
US-Flight-Delay-Analytics/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── notebooks/
│   ├── Flight_ETL.ipynb
│   └── Data_Cleaning.ipynb
│
├── powerbi/
│   └── US_Flight_Analytics.pbix
│
├── dashboard_images/
│   ├── overview.png
│   ├── airline_analysis.png
│   ├── airport_analysis.png
│   └── delay_analysis.png
│
├── sql/
│   └── analytical_queries.sql
│
└── README.md
```

---

## 📊 Dashboard Features

### Executive Overview

- Total Flights
- Delayed Flights
- Cancelled Flights
- Average Arrival Delay
- Average Departure Delay

---

### Airline Performance

- Flights by Airline
- Average Delay by Airline
- Cancellation Rate
- Top Performing Airlines
- Worst Performing Airlines

---

### Airport Analysis

- Busiest Airports
- Airport Delay Distribution
- Arrival vs Departure Delays
- Airport Performance Comparison

---

### Delay Analysis

- Carrier Delays
- Weather Delays
- NAS Delays
- Security Delays
- Late Aircraft Delays

---

### Time-Based Analysis

- Monthly Flight Trends
- Monthly Delay Trends
- Day-of-Week Analysis
- Seasonal Performance

---

## 🔄 ETL Pipeline

### Step 1 – Data Ingestion

- Imported raw CSV datasets into Databricks.
- Loaded data using PySpark DataFrames.

### Step 2 – Data Cleaning

- Removed duplicate records.
- Handled missing values.
- Standardized column names.
- Corrected data types.

### Step 3 – Feature Engineering

Created analytical columns including:

- Delay Status
- Flight Duration
- Cancellation Flag
- Total Delay
- Delay Category

### Step 4 – Delta Lake Storage

Processed data was stored as Delta tables for:

- ACID Transactions
- Efficient Querying
- Optimized Analytics

### Step 5 – Dashboard Development

Connected Delta tables to Power BI and created interactive dashboards.

---

## 📈 Key Insights

- Identification of airlines with the highest on-time performance.
- Airports experiencing the most frequent delays.
- Seasonal impact on flight operations.
- Most common reasons for flight delays.
- Monthly cancellation trends.
- Peak operational periods.

---

## 📷 Dashboard Preview

### Executive Dashboard

_Add dashboard screenshot here_

---

### Airline Analysis

_Add dashboard screenshot here_

---

### Airport Analysis

_Add dashboard screenshot here_

---

### Delay Analysis

_Add dashboard screenshot here_

---

## 📚 Dataset

The project uses publicly available flight performance data from:

**US Bureau of Transportation Statistics (BTS)**

https://www.transtats.bts.gov/

---

## 🚀 Future Enhancements

- Real-time flight streaming with Apache Kafka
- Automated ETL scheduling
- Weather data integration
- Predictive delay modeling using Machine Learning
- Azure Data Factory orchestration
- Cloud deployment on Azure or AWS

---

## 💡 Skills Demonstrated

- Data Engineering
- ETL Pipeline Development
- PySpark
- Apache Spark
- Delta Lake
- Data Cleaning
- Data Transformation
- Data Modeling
- Power BI Dashboarding
- Business Intelligence
- Analytical SQL
- Feature Engineering

---

## 👨‍💻 Author

**Deepak Jhinjha**

GitHub: https://github.com/YourGitHubUsername

LinkedIn: https://linkedin.com/in/YourLinkedIn

---

## ⭐ If you found this project useful, consider giving it a Star.
