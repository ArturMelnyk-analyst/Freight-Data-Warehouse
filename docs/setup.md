# Setup Guide — FAF Freight Data Warehouse

This guide explains how to configure and run the project locally.

---

## Requirements

- Python 3.11+
- MySQL 8.0+
- MySQL Workbench (recommended)
- Git

---

## Python Dependencies

Install required packages:

```bash
pip install pandas sqlalchemy pymysql python-dotenv
```

Or:

```bash
pip install -r requirements.txt
```

## Environment Variables

Create a .env file in the project root.

Example:

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=faf_dw
```

## Database Setup

Open MySQL Workbench.

Create the database:

```sql
CREATE DATABASE faf_dw;
```

Run DDL scripts from `sql/ddl/` in order.


## Pipeline Execution

Run the full ETL pipeline:

```bash
python -m etl.run_all
```

Or on Windows:

run_project.bat

## Validation

Run validation separately if needed:

python -m etl.05_validate

## Analytics

Open sql/analytics/00_params.sql.

Set values for:

@YEAR

@Y1

@Y2

Execute the desired analytics query files in MySQL Workbench.


## Performance Scripts

Performance scripts are in:

sql/perf/

Use them to reproduce index decisions and EXPLAIN ANALYZE evidence.