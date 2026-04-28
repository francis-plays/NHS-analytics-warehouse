

```
# NHS A&E Analytics Data Warehouse

A dimensional data warehouse analyzing NHS A&E waiting times performance against the 4-hour target (95% of patients seen within 4 hours).

## Project Overview

**Objective:** Build a star schema data warehouse to identify which NHS hospitals are meeting the 4-hour A&E waiting time target.

**Data Source:** NHS England A&E Attendances and Emergency Admissions (October 2025 - March 2026)

**Tech Stack:**
- **Cloud Storage:** AWS S3
- **Data Warehouse:** Snowflake
- **Transformation:** dbt (data build tool)
- **Language:** Python, SQL

---

## Architecture

```
Raw Data (S3) → Python ETL → Snowflake (Raw) → dbt (Transformations) → Analytics
```

### Star Schema Design

```
        DIM_TRUST (122 hospitals)
              ↓
    FACT_AE_ATTENDANCE (732 rows)
              ↓
        DIM_DATE (6 months)
```

---

## Key Findings

### 1. System-Wide Failure
- **Target:** 95% of patients seen within 4 hours
- **Reality:** National average is **59.2%**
- **All 122 hospitals are failing the target**

### 2. Worst-Performing Hospitals
1. University Hospitals Plymouth — 40.0%
2. Nottingham University Hospitals — 41.7%
3. Shrewsbury and Telford — 44.6%

### 3. Winter Crisis is Real
- **October 2025:** 59.4%
- **January 2026:** 56.5% (worst month)
- **March 2026:** 63.5% (recovery)

### 4. Geographic Inequality
- **Worst region:** South West (55.4%)
- **Best region:** London (62.2% — still failing)

---

## Data Pipeline

### Phase 1: Data Loading
1. Downloaded 6 months of NHS A&E data (CSV format)
2. Uploaded raw files to S3 bucket
3. Python script extracts, cleans, and loads to Snowflake raw table

**Key cleaning steps:**
- Removed TOTAL aggregate rows
- Filtered out zero-attendance records
- Standardized column names

### Phase 2: dbt Transformation (Star Schema)

**Staging Layer:**
- `stg_ae_attendance` — cleaned raw data, calculated metrics

**Dimension Tables:**
- `dim_trust` — 122 hospitals with surrogate keys
- `dim_date` — 6 months with surrogate keys

**Fact Table:**
- `fact_ae_attendance` — 732 rows (122 hospitals × 6 months)
- Metrics: total_attendances, over_4hrs, pct_seen_4hrs

**Marts Layer (Analytics):**
- `mart_trust_performance` — hospital rankings
- `mart_monthly_trends` — seasonal patterns
- `mart_regional_performance` — geographic analysis

---

## Sample Queries

### Which hospitals are failing worst?

```sql
SELECT 
    trust_name,
    avg_4hr_performance,
    total_attendances
FROM mart_trust_performance
WHERE target_status = 'FAIL'
ORDER BY avg_4hr_performance ASC
LIMIT 10;
```

### Is January worse than October?

```sql
SELECT 
    month_name,
    year,
    avg_4hr_performance
FROM mart_monthly_trends
ORDER BY year, month_name;
```

### Which regions need the most support?

```sql
SELECT 
    region,
    avg_4hr_performance,
    num_trusts
FROM mart_regional_performance
ORDER BY avg_4hr_performance ASC;
```

---

## Project Structure

```
NHS-analytics-warehouse/
├── config/
│   └── snowflake_config.py     # Snowflake connection settings
├── scripts/
│   └── load.py                 # Python ETL script
├── nhs_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   └── stg_ae_attendance.sql
│   │   ├── dimensions/
│   │   │   ├── dim_trust.sql
│   │   │   └── dim_date.sql
│   │   ├── facts/
│   │   │   └── fact_ae_attendance.sql
│   │   └── marts/
│   │       ├── mart_trust_performance.sql
│   │       ├── mart_monthly_trends.sql
│   │       └── mart_regional_performance.sql
│   └── dbt_project.yml
└── README.md
```

---

## Key Concepts Demonstrated

- **Dimensional Modeling:** Star schema with fact and dimension tables
- **Surrogate Keys:** Auto-generated integer keys for dimensions
- **ETL Pipeline:** Extract (S3) → Transform (dbt) → Load (Snowflake)
- **Data Quality:** Handling duplicates, null values, aggregate rows
- **Business Intelligence:** Pre-built analytical queries (marts)

---

## How to Run

### Prerequisites
- AWS account with S3 access
- Snowflake account
- Python 3.8+
- dbt installed (`pip install dbt-snowflake`)

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/francis-plays/NHS-analytics-warehouse.git
cd NHS-analytics-warehouse
```

2. **Set up environment variables**
```bash
# Create .env file with:
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
```

3. **Run the ETL pipeline**
```bash
python scripts/load.py
```

4. **Run dbt transformations**
```bash
cd nhs_dbt
dbt run
```

---

## Future Enhancements

- Add Airflow orchestration for automated monthly refreshes
- Build Streamlit dashboard for visualization
- Implement slowly changing dimensions (SCD Type 2) for tracking hospital name changes
- Add data quality tests using dbt tests
- Expand analysis to include demographic breakdowns

---

## Author

**Francisxi6**  
Data Engineering Portfolio Project  
[GitHub](https://github.com/francis-plays)

---

## Data Source

NHS England - A&E Attendances and Emergency Admissions  
https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/
```

---