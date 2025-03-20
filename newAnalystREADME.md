# Data-Ngin Developer Guide

## Why Python?

We chose **Python** for several key reasons:

1. **Rapid Development & Prototyping**

   - Extensive ecosystem for data handling (`pandas`, `NumPy`, `SQLAlchemy`).
   - Simple syntax for fast development and iteration.
   - Strong support for Jupyter Notebooks for testing and debugging.

2. **Integration with Financial Data Providers**

   - Easy API interactions with **Databento, FRED, Interactive Brokers, Charles Schwab, etc.**
   - Strong database support with **SQLAlchemy and psycopg2**.
   - Seamless integration with **Apache Airflow** for automation.

3. **Scalability & Modularity**

   - **Modular architecture** allows adding new fetchers, cleaners, and inserters without modifying core logic.
   - **Encapsulation** ensures that changes to one module don’t break others.
   - **Easier debugging and testing**—each module can be tested in isolation.

---

## Getting Started

### Prerequisites

1. **Development Environment Setup**

   ```bash
   # Required tools
   - Python 3.10+
   - PostgreSQL 16+
   - Docker & Docker Compose
   - Poetry (dependency management)
   - Apache Airflow
   - VS Code
   ```

2. **Required Libraries**
   
   ```bash
   # Install dependencies
   poetry install
   ```

   **Core Dependencies:**

   - `pandas`, `NumPy` (data processing)
   - `SQLAlchemy`, `psycopg2` (database connectivity)
   - `databento`, `ib_insync` (financial data providers)
   - `Airflow` (automation & scheduling)

### Why Poetry?

**Poetry** is a dependency management and packaging tool for Python that offers significant advantages over `pip`:

1. **Dependency Resolution:** Poetry resolves dependencies more efficiently, avoiding conflicts that `pip` sometimes overlooks.
2. **Virtual Environment Management:** It automatically creates and manages virtual environments, isolating project dependencies.
3. **Project Metadata:** The `pyproject.toml` file consolidates dependency definitions and project configuration, simplifying project setup.

By using Poetry, we ensure reproducibility across environments and streamline the development workflow.

### Building Your First Component

1. **Clone and Build**

   ```bash
   git clone either forked repo or repo
   cd data-ngin
   poetry install
   ```

2. **Run Tests**

   ```bash
   poetry run pytest tests/ or poetry run unittest
   ```

---

## System Overview

### How Data-Ngin Works

Data-Ngin is a modular pipeline designed to fetch, clean, and store financial market data from multiple sources into a PostgreSQL/TimescaleDB database.

**Core Flow:**

```
Data Provider → Fetcher → Cleaner → Inserter → PostgreSQL
```

**Example Workflow (Databento):**

1. **Loader:** Reads contract symbols from a CSV or database.
2. **Fetcher:** Calls the Databento API to retrieve raw OHLCV data.
3. **Cleaner:** Standardizes timestamps, validates missing values, and structures the dataset.
4. **Inserter:** Writes cleaned data into the PostgreSQL database.
5. **Airflow DAG:** Automates the entire process on a schedule.

### Understanding the Tools

#### What is TimescaleDB?

TimescaleDB is a time-series extension for PostgreSQL that optimizes performance for handling large amounts of time-ordered data. It supports automatic partitioning (hypertables), fast aggregations, and compression, making it ideal for storing OHLCV market data.

#### What is psycopg2?

`psycopg2` is a Python adapter for PostgreSQL, allowing direct execution of SQL queries from Python scripts. It enables secure, parameterized queries, preventing SQL injection and improving performance.

#### What is SQLAlchemy?

SQLAlchemy is an ORM (Object-Relational Mapper) that simplifies database interactions in Python. Instead of writing raw SQL, developers can use Python classes and objects to manage database operations, improving readability and maintainability.

#### What is Apache Airflow?

Apache Airflow is an orchestration tool that automates the execution of data pipelines. It schedules, monitors, and logs the execution of tasks, ensuring that market data is fetched and stored on a regular schedule without manual intervention.

---

## Code Structure

```
data_ngin/
├── dags/                  # Airflow DAGs (pipeline automation)
├── data/                  # Core pipeline logic
│   ├── modules/           # Fetchers, loaders, cleaners, inserters
│   ├── config/            # Config files
│   ├── orchestrator.py    # Main execution manager
├── tests/                 # Unit tests for all components
├── utils/                 # Utility functions
├── README.md              # Documentation
├── poetry.lock            # Dependency management
└── pyproject.toml         # Project metadata
```

---

## Core Components

### 1. Data Loader (`data/modules/loader.py`)

**Purpose:**

- Loads metadata (contracts, symbols, asset types) from a CSV file or PostgreSQL database.
- Ensures symbols are valid before fetching market data.

**Implementation:**

```python
class Loader(ABC):
    """Abstract base class for all loaders."""
    @abstractmethod
    def load_symbols(self) -> Dict[str, str]:
        pass

class CSVLoader(Loader):
    """Loads contract symbols from a CSV file."""
    def load_symbols(self) -> Dict[str, str]:
        df = pd.read_csv("contracts/contract_valid.csv")
        return dict(zip(df["symbol"], df["asset_type"]))
```

### 2. Data Fetcher (`data/modules/fetcher.py`)

**Purpose:**

- Connects to financial data providers (Databento, FRED, IBKR).
- Retrieves OHLCV data for a given symbol and date range.

**Example: Databento Fetcher (`databento_fetcher.py`):**

```python
class DatabentoFetcher(Fetcher):
    """Fetches historical market data from Databento."""
    def fetch_data(self, symbol: str, start_date: str, end_date: str):
        data = self.client.timeseries.get_range_async(symbol, start_date, end_date)
        return data.to_df()
```

### 3. Data Cleaner (`data/modules/cleaner.py`)

**Purpose:**

- Validates required fields (OHLCV structure).
- Handles missing values (forward-fill, drop NaN, etc.).
- Converts timestamps to UTC format.

**Implementation:**

```python
class DatabentoCleaner(Cleaner):
    def clean(self, data: pd.DataFrame) -> pd.DataFrame:
        data["time"] = pd.to_datetime(data["time"]).dt.tz_localize("UTC")
        data = data.dropna()  # Drop missing values
        return data
```

### 4. Airflow DAG (`dags/data_pipeline_dag.py`)

**Purpose:**

- Automates daily data ingestion.
- Runs `orchestrator.py` at scheduled intervals.

**Implementation:**

```python
default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}
dag = DAG("data_pipeline", default_args=default_args, schedule_interval="@daily")

task = PythonOperator(task_id="fetch_data", python_callable=orchestrator.run, dag=dag)
```

---

## Common Development Tasks

1. **Run the pipeline manually**

```bash
poetry run python data/orchestrator.py
```

2. **Check database contents**

```sql
SELECT * FROM futures_data.ohlcv_1d LIMIT 10;
```

3. **Trigger Airflow DAG**

```bash
airflow dags trigger data_pipeline
```

---

## Importance of Unit Testing

Unit testing is essential for maintaining code quality and ensuring that individual components of the Data-Ngin pipeline function correctly. Key benefits include:

1. **Early Bug Detection:** Catch bugs early in development, reducing the cost of fixing issues.
2. **Code Refactoring Confidence:** Safely refactor code without fear of breaking functionality.
3. **Improved Documentation:** Tests act as executable documentation for expected behavior.

Running `unitttest/pytest` regularly ensures stability and robustness across the project.

```bash
poetry run pytest tests/
```

---

## Abstract Base Classes (ABC)

Abstract Base Classes (ABCs) define a common interface for a group of related classes. They cannot be instantiated directly but provide a blueprint for subclasses.

**Why Use ABCs?**
1. **Consistency:** Ensure that all subclasses implement required methods.
2. **Error Prevention:** Prevent instantiation of incomplete implementations.
3. **Improved Readability:** Make code structure clearer.

**Example:**
```python
from abc import ABC, abstractmethod

class Fetcher(ABC):
    @abstractmethod
    def fetch_data(self, symbol: str, start_date: str, end_date: str):
        pass
```

In the above example, any subclass of `Fetcher` must implement the `fetch_data` method.

---
## Accessing the Database (pgAdmin)

To manage the PostgreSQL database, use **pgAdmin**:

1. **Install:** [Download pgAdmin](https://www.pgadmin.org/download/).
2. **Connect:** In pgAdmin, right-click *Servers* → **Create > Server**.
3. **Connection Details:**  
   - Fill in **Host**, **Port**, **Username**, and **Password**.  
   - If you don’t have these details, contact the **Data Team Lead**.
4. **Running Queries:**
   - In the left sidebar, expand the connected server.
   - Navigate to `Databases > algo_data > Schemas > Tables` to explore tables.
   - Right-click the `datangin` database and select **Query Tool**.
   - In the query editor, enter your SQL commands. 

## Helpful SQL Queries

Here are some common SQL queries for managing and querying the database:

1. **View Table Structure:**
```sql
SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'ohlcv_1d';
```

2. **Retrieve Latest Data:**
```sql
SELECT * FROM futures_data.ohlcv_1d ORDER BY time DESC LIMIT 10;
```

3. **Count Rows in a Table:**
```sql
SELECT COUNT(*) FROM futures_data.ohlcv_1d;
```

4. **Find Missing Data:**
```sql
SELECT * FROM futures_data.ohlcv_1d WHERE close IS NULL;
```

5. **Aggregate Data by Month:**
```sql
SELECT DATE_TRUNC('month', time) AS month, AVG(close) AS avg_close
FROM futures_data.ohlcv_1d
GROUP BY month
ORDER BY month;
```

# Good Git Practices

Following good Git practices ensures clean, maintainable code, reduces bugs, and streamlines collaboration. Here’s a quick guide to efficient Git workflows.

---

## 1. Create a Feature Branch

Always create a new branch for each task or feature instead of working directly on the `main` branch.

**Why?**  
- Keeps `main` clean and stable.  
- Allows parallel development without conflicts.  
- Easier to roll back if something goes wrong.

**How?**
```bash
# Fetch the latest changes from main
git checkout main
git pull origin main

# Create and switch to a new branch
git checkout -b feature/your-feature-name
```
##  4. Submit a Pull Request (PR)

Create a Pull Request (PR) to merge your branch into `main`.

**Why?**  
- Enables code review for catching bugs and improving quality.  
- Ensures all tests pass before merging.  
- Provides a clear history of changes.

**How?**
1. Go to the GitHub/GitLab repository.  
2. Click **"New Pull Request"**.  
3. Set the base branch as `main` and the compare branch as `feature/your-feature-name`.  
4. Write a clear PR title and description.  
5. Request reviewers and submit the PR.  

---

## 5. Code Review & Merge

Wait for reviews, address feedback, and merge when approved.

**Why?**  
- Ensures high-quality code.  
- Encourages knowledge sharing.  

**How?**
```bash
# After PR approval
git checkout main
git pull origin main
git merge feature/your-feature-name
```
# EC2 Instances

The entire Data-Ngin infrastructure runs on an **Amazon EC2 (Elastic Compute Cloud)** instance. This provides a scalable, secure, and reliable environment for running our data pipelines and database services.

---

## What is an EC2 Instance?

Amazon EC2 is a cloud-based virtual server provided by AWS. It allows users to run applications and services without managing physical hardware.

**Key Features:**
1. **Scalability:** Easily adjust computing resources based on workload.  
2. **Reliability:** High uptime with backup and recovery options.  
3. **Security:** Managed through SSH keys and AWS security groups.  
4. **Cost-Effective:** Pay only for what you use.  

---

## Why EC2 Matters for Data-Ngin

1. **Centralized Environment:**  
   The EC2 instance acts as the core hub for running the Data-Ngin pipelines, PostgreSQL database, and Apache Airflow. This ensures consistent performance and reduces dependency on local machines.

2. **Continuous Availability:**  
   The EC2 instance runs 24/7, allowing pipelines to fetch, clean, and insert data automatically without manual intervention.

3. **Remote Access:**  
   You can securely connect to the instance via SSH for updates, debugging, and monitoring.

---

