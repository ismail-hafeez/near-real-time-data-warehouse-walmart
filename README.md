# Near-Real-Time Data Warehouse - Walmart

A prototype data warehouse system implementing the HYBRIDJOIN algorithm for near-real-time ETL processing of Walmart sales transactions.

## Overview

This project implements a star schema data warehouse with:
- **HYBRIDJOIN Algorithm**: Stream-based join for combining fast-arriving transactional data with large disk-based master data
- **Star Schema**: Fact and dimension tables for multidimensional analysis
- **OLAP Queries**: 20 analytical queries with data visualizations
- **ETL Pipeline**: Automated extraction, transformation, and loading of sales data

## Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Database**
   - Update database credentials in `src/db_config/dw_config.py` and `src/hybrid_join/main.py`
   - Ensure MySQL server is running

3. **Create Data Warehouse**
   ```bash
   python src/db_config/dw_config.py
   ```

## Usage

### Run ETL Process
```bash
python src/hybrid_join/main.py
```

### Execute OLAP Queries
Open `src/sql_queries/data_visualization.ipynb` in Jupyter Notebook and run all cells to execute 20 queries with visualizations.

## Project Structure

```
root/
    - data/                          # CSV data files
        - transactional_data.csv
        - customer_master_data.csv
        - product_master_data.csv
    - logs/                          # Application logs
    - report/                        # Project documentation
        - Project-Report.pdf
    - schema/                        # Database schema diagrams
        - star_schema.png
    - src/
        - db_config/                 # Database setup
            - createDW.sql           # Star schema DDL
            - dw_config.py           # Dimension table population
        - hybrid_join/               # HYBRIDJOIN implementation
            - main.py                # ETL orchestration
            - hash_table.py          # Hash table component
            - stream_buffer.py       # Stream buffer
            - disk_buffer.py         # Disk buffer
            - queue.py               # FIFO queue
        - sql_queries/               # OLAP analysis
            - queries.sql            # 20 analytical queries
            - data_visualization.ipynb # Query execution & visualization
    - requirements.txt              # Python dependencies
```

## Key Components

- **HYBRIDJOIN**: Stream-based join algorithm for real-time data processing
- **Star Schema**: FactSales fact table with 5 dimension tables (Customer, Product, Date, Store, Supplier)
- **ETL Pipeline**: Automated data loading from CSV to data warehouse
- **Data Visualization**: Jupyter notebook with 20 OLAP queries and charts

## Technologies

- Python 3.x
- MySQL
- Pandas, NumPy
- Matplotlib, Seaborn
- Jupyter Notebook
