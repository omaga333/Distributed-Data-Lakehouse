# 🏛️ Distributed Data Lakehouse

### Enterprise-Grade Lakehouse Architecture Powered by Trino, Apache Iceberg, Project Nessie, dbt Core & Apache Airflow

  A modern, decoupled, Git-for-Data lakehouse implementing the Medallion Architecture (Bronze, Silver, Gold) with distributed query execution, ACID transactions, and automated orchestration[cite: 2].















  
    https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apache/apache-original.svg" width="55" height="55" alt="Airflow"/>Airflow
    https://trino.io/assets/images/trino-logo.png" width="55" height="55" alt="Trino"/>Trino
    https://iceberg.apache.org/assets/images/iceberg-logo-icon.png" width="55" height="55" alt="Iceberg"/>Iceberg
    https://projectnessie.org/img/nessie-logo.svg" width="55" height="55" alt="Nessie"/>Nessie
    https://cdn.jsdelivr.net/gh/devicons/devicon/icons/dbt/dbt-original.svg" width="55" height="55" alt="dbt"/>dbt Core
    https://min.io/resources/img/logo/MINIO_Bird.png" width="55" height="55" alt="MinIO"/>MinIO
    https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" width="55" height="55" alt="PostgreSQL"/>Postgres
    https://cdn.jsdelivr.net/gh/devicons/devicon/icons/redis/redis-original.svg" width="55" height="55" alt="Redis"/>Redis
    https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" width="55" height="55" alt="Docker"/>Docker
    https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" width="55" height="55" alt="Python"/>Python
  




https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" />

## 📖 Table of Contents

- [Executive Summary](#-executive-summary)
- [Architecture & Core Concepts](#️-distributed-data-lakehouse-architecture)
- [End-to-End Data Workflow](#-end-to-end-data-workflow)
- [The Medallion Data Model](#-the-medallion-data-model)
- [Dockerized Infrastructure & Networking](#-dockerized-infrastructure--networking)
- [Service Endpoints](#-service-endpoints)
- [Deep Component Breakdown](#-deep-component-breakdown)
- [Project Structure](#-project-structure)
- [Quick Start & Deployment Guide](#-quick-start)
- [Validation & Smoke Tests](#-validation--smoke-tests)
- [Engineering Highlights](#-engineering-highlights)
- [Troubleshooting](#-troubleshooting)
- [Security](#-security)
- [Roadmap](#-roadmap)

https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" />

## 📌 Executive Summary

The **Distributed Data Lakehouse** is an end-to-end analytical data platform designed to process high-volume E-commerce data[cite: 2]. It completely decouples storage, metadata, compute, and orchestration, resolving the limitations of traditional Data Warehouses (inflexibility, high cost) and Data Lakes (data swamps, lack of ACID guarantees)[cite: 2]. 

At its core, this project demonstrates a highly robust **ELT pipeline**:
*   **Storage & Format**: Raw and processed data are stored in **MinIO** as highly optimized Parquet files managed by **Apache Iceberg** table formats[cite: 2].
*   **Compute & Catalog**: **Trino** executes all distributed SQL queries against the data, resolving table states and branches (e.g., `main` vs `dev`) through the **Project Nessie** catalog[cite: 2]. 
*   **Transformation & Orchestration**: **dbt Core** handles the Medallion transformation logic (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), meticulously orchestrated by an **Apache Airflow** DAG using a custom-built `DbtOperator`[cite: 2].

https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" />

## 🏗️ Distributed Data Lakehouse Architecture

```mermaid
flowchart TB
    subgraph Orchestration["Orchestration Layer (Airflow Cluster)"]
        AF_API["Airflow API Server"]
        AF_SCHED["Airflow Scheduler"]
        AF_DAG["Airflow DAG Processor"]
        AF_WORKER["Celery Worker\n(Executes Custom DbtOperator)"]
        PG[(PostgreSQL 13\nMetadata DB)]
        REDIS[(Redis 7.2\nCelery Broker)]

        AF_SCHED --> REDIS
        AF_WORKER --> REDIS
        AF_SCHED --> PG
        AF_WORKER --> PG
        AF_API --> PG
        AF_DAG --> AF_SCHED
    end

    subgraph Compute["Distributed Compute Layer (Trino)"]
        TRINO_COORD["Trino Coordinator\n(Port: 8080 internal / 9080 host)"]
        TRINO_WORKER["Trino Worker 1\n(Task Executio








 Coordinator\n(Port: 8080 internal / 9080 host)"]
        TRINO_WORKER["Trino Worker 1\n(Task Execution Pool)"]
        TRINO_COORD <--> TRINO_WORKER
    end

    subgraph Catalog["Metadata & Catalog Layer"]
        NESSIE["Project Nessie 0.76.6\n(Port: 19120)\nBranching: main / dev"]
    end

    subgraph Storage["Open Table Format & Object Storage"]
        ICEBERG["Apache Iceberg v2\n(Snapshots | Manifests | Metadata)"]
        MINIO[("MinIO S3-Compatible Object Store\n(Port: 9000 API / 9001 Console)")]
        PARQUET["Columnar Parquet Files"]

        ICEBERG --> PARQUET
        PARQUET --> MINIO
    end

    AF_WORKER -- "Dispatches dbt SQL via DbtOperator" --> TRINO_COORD
    TRINO_COORD -- "Resolves table pointers & snapshots" --> NESSIE
    NESSIE -- "Points to current JSON metadata" --> ICEBERG
    TRINO_COORD -- "Reads/Writes Parquet via Native S3" --> MINIO
    TRINO_WORKER -- "Executes partition scans" --> MINIO

    classDef orchestrate fill:#017CEE,stroke:#0B4C8C,stroke-width:2px,color:#fff;
    classDef compute fill:#DD00A1,stroke:#8A0064,stroke-width:2px,color:#fff;
    classDef catalog fill:#00A4A6,stroke:#005B5C,stroke-width:2px,color:#fff;
    classDef storage fill:#C72C48,stroke:#7D182A,stroke-width:2px,color:#fff;

    class AF_API,AF_SCHED,AF_DAG,AF_WORKER,AF_TRIG,PG,REDIS orchestrate;
    class TRINO_COORD,TRINO_WORKER compute;
    class NESSIE catalog;
    class ICEBERG,MINIO,PARQUET storage;
```

https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" />

## 🔄 End-to-End Data Workflow

The data pipeline runs on a scheduled cadence (every 15 minutes) processing thousands of E-commerce events[cite: 2].

```mermaid
sequenceDiagram
    autonumber
    participant AF as Airflow
    participant DBT as dbt Core
    participant TR as Trino
    participant NES as Nessie
    participant S3 as MinIO

    AF->>AF: execute start_pipeline()
    AF->>DBT: run seed_bronze() via DbtOperator
    DBT->>TR: Check for existing raw data
    opt If no data
        DBT->>TR: dbt seed (Load raw E-commerce CSVs)
        TR->>S3: Write raw Parquet
    end
    
    Note over AF,S3: Bronze Layer Processing
    AF->>DBT: dbt run --select tag:bronze
    DBT->>TR: Standardize types, handle NULLs
    TR->>NES: Commit Snapshot to 'main'
    AF->>AF: validate_bronze_data()
    
    Note over AF,S3: Silver Layer Processing
    AF->>DBT: dbt run --select tag:silver
    DBT->>TR: Apply business logic, sessionization, joins
    TR->>S3: Write cleaned Parquet files
    TR->>NES: Commit Snapshot to 'main'
    AF->>AF: validate_silver_data()
    
    Note over AF,S3: Gold Layer Processing
    AF->>DBT: dbt run --select tag:gold
    DBT->>TR: Calculate metrics, KPIs, daily aggregates
    TR->>S3: Write aggregated Parquet files
    TR->>NES: Commit Snapshot to 'main'
    AF->>AF: validate_gold_data()
    
    AF->>DBT: dbt docs generate
    AF->>AF: end_pipeline()
```

https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" />

## 📊 The Medallion Data Model

The lakehouse adopts the Medallion Architecture to progressively improve data structure, ensuring analytics dashboards only read optimized, heavily-aggregated data[cite: 2]. 

| Layer | Objective & Transformations[cite: 2] | Examples from E-commerce Data[cite: 2] |
| :--- | :--- | :--- |
| **Bronze** 🥉 | Stores raw source data. Modifies timestamps, handles empty strings (`''` to `NULL`), and adds metadata (`ingested_at`, `source_system`). | `bronze_customer_events`, `bronze_inventory_snapshots`, `bronze_payment_transactions`, `bronze_support_tickets` |
| **Silver** 🥈 | Data cleaning, standardizing, and applying business logic. Handles sessionization (grouping user clicks), risk profiling for payments, and stock categorizations (Out of Stock, Low Stock). | `silver_customer_sessions`, `silver_inventory_status`, `silver_payment_profiles`, `silver_support_metrics` |
| **Gold** 🥇 | Business-ready data. Pre-calculates heavy metrics, KPIs, aggregations, and daily rollups. Ready for BI tools (Power BI, Tableau) to consume directly without processing lag. | `gold_daily_metrics`, `gold_customer_summary`, `gold_product_summary` |

https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" />

## 🐳 Dockerized Infrastructure & Networking

The environment is containerized via Docker Compose. Understanding the networking rules is critical:
*   **Host Networking:** For accessing UIs from your local browser (e.g., `localhost:9080` for Trino).
*   **Container Networking:** For services communicating internally (e.g., Airflow accessing Trino via `trino-coordinator:8080`)[cite: 2].

| Service | Role | Host Port | Container Port | Exposure | Notes |
| :--- | :--- | :---: | :---: | :--- | :--- |
| `postgres` | Airflow Metadata DB | - | `5432` | Internal | Stores Airflow DAG states[cite: 2] |
| `redis` | Celery Broker | - | `6379` | Internal | Message broker for Celery[cite: 2] |
| `minio` | S3 Object Storage | `9000`, `9001` | `9000`, `9001` | **External** | API (9000), Console (9001)[cite: 2] |
| `nessie-catalog` | Iceberg Catalog | `19120` | `19120` | **External** | Branching/Versioning[cite: 2] |
| `trino-coordinator` | Query Coordinator | `9080` | `8080` | **External** | Host port mapped to 9080 to avoid Airflow 8080 conflict[cite: 2] |
| `trino-worker-1` | Query Worker | - | `8080` | Internal | Executes Trino tasks[cite: 2] |
| `airflow-apiserver` | Airflow UI/API | `8080` | `8080` | **External** | Airflow 3.0.6[cite: 2] |
| `airflow-worker` | Task Executor | - | - | Internal | Runs custom DbtOperator[cite: 2] |
| *(Other Airflow)* | Scheduler, DAG processor | - | - | Internal | Orchestration background services |



## 🔌 Service Endpoints

| Service | Host URL | Container URL | Purpose |
| :--- | :--- | :--- | :--- |
| **Airflow UI** | http://localhost:8080 | `http://airflow-apiserver:8080` | Monitor DAG runs and task logs. |
| **Trino UI** | http://localhost:9080 | `http://trino-coordinator:8080` | View query plans and worker stats. |
| **MinIO API** | http://localhost:9000 | `http://minio:9000` | S3 API Endpoint for Iceberg tables[cite: 2]. |
| **MinIO Console** | http://localhost:9001 | `http://minio:9001` | Object browser UI[cite: 2]. |
| **Nessie Catalog**| http://localhost:19120 | `http://nessie-catalog:19120` | Catalog REST API[cite: 2]. |

https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" />

## 🔍 Deep Component Breakdown

### 🌬️ Apache Airflow & Custom `DbtOperator`
Airflow acts strictly as an orchestrator ("When and how to execute") rather than performing data transformations itself[cite: 2]. 
To bridge Airflow and dbt elegantly, a custom **`DbtOperator`** was developed in Python extending Airflow's `BaseOperator`[cite: 2]. This class uses the `dbtRunner` API to execute dbt commands directly without relying on clumsy subshells[cite: 2].

### 🛠️ dbt Core (Data Build Tool)
dbt handles all data transformations ("How the data is modified")[cite: 2]. 
*   **Tags & Execution**: The Airflow DAG triggers dbt models sequentially using tags (`dbt run --select tag:bronze`, then `tag:silver`, then `tag:gold`)[cite: 2].
*   **Testing**: Data quality assertions (`unique`, `not_null`) are defined in `schema.yml` and run against the Iceberg tables via `dbt test` to ensure referential integrity[cite: 2].

### ⚡ Trino (Query Engine)
Trino is the distributed SQL engine executing the actual queries ("Who executes the SQL")[cite: 2]. It is connected to the Iceberg catalog via its `iceberg.properties` configuration[cite: 2]:
```properties
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=http://nessie-catalog:19120/api/v1
iceberg.nessie-catalog.ref=main
iceberg.nessie-catalog.default-warehouse-dir=s3://lakehouse
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
```

### 🧊 Project Nessie & Apache Iceberg
*   **Iceberg**: Manages the open table format metadata (Snapshots $\rightarrow$ Manifest Lists $\rightarrow$ Manifest Files $\rightarrow$ Parquet) allowing for data skipping and time travel[cite: 2].
*   **Nessie**: Acts as "Git for Data". It allows Trino to point to different branches of the catalog (e.g., `main` vs `dev`)[cite: 2]. You can isolate development transformations in the `dev` branch without affecting production `main` data[cite: 2].

https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" />

## 📁 Project Structure

```text
.
├── dags/
│   ├── ecommerce_dag.py          # Primary medallion Airflow DAG
│   └── ecommerce_dbt/            # Complete dbt Core project
│       ├── dbt_project.yml       # dbt configurations, tags, and materializations
│       ├── profiles.yml          # Trino coordinator connection settings
│       ├── models/
│       │   ├── bronze/           # Source schema transformations (CSV -> Iceberg)
│       │   │   ├── bronze_customer_events.sql
│       │   │   ├── bronze_inventory_snapshots.sql
│       │   │   ├── bronze_payment_transactions.sql
│       │   │   ├── bronze_support_tickets.sql
│       │   │   └── schema.yml    # Data quality tests (unique, not_null)
│       │   ├── silver/           # Enriched business entities & sessionization
│       │   └── gold/             # Aggregated KPI data marts & BI models
│       └── seeds/                # Raw E-commerce seed CSVs
├── config/                       # Airflow & service configuration mounts
├── trino/
│   └── catalog/
│       └── iceberg.properties    # Trino Iceberg-Nessie-MinIO catalog connector
├── Dockerfile                    # Custom Airflow image with dbt-trino & dependencies
├── docker-compose.yaml           # Full infrastructure orchestration
├── pyproject.toml                # Project dependency manifest
├── requirements.txt              # Pinned Python package dependencies (Airflow 3.0.6, etc.)
└── README.me





``` https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" /> ## 🚀 Quick Start ### 1. Clone & Bootstrap Stack ```bash git clone https://github.com/omaga333/Distributed-Data-Lakehouse.git cd Distributed-Data-Lakehouse # Build custom images and start the entire cluster in detached mode docker compose up -d --build # Verify container statuses docker compose ps ``` ### 2. Verify Services ```bash # Check Nessie Catalog startup docker compose logs nessie-catalog --tail 100 # Check Trino Coordinator docker compose logs trino-coordinator --tail 100 # Check Airflow Worker readiness docker compose logs airflow-worker --tail 100 ``` ### 3. Cleanup ```bash # Stop containers and wipe volumes for a clean slate docker compose down -v ``` https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" /> ## ✅ Validation & Smoke Tests ### Validating Trino, Nessie & MinIO Integration You can use any SQL Client (like DBeaver) connecting to `localhost:9080` or use the Trino CLI: ```bash docker compose exec trino-coordinator trino ``` Run these commands to validate catalog connections: ```sql SHOW CATALOGS; SHOW SCHEMAS FROM iceberg; CREATE SCHEMA IF NOT EXISTS iceberg.lakehouse; CREATE TABLE iceberg.lakehouse.test_table ( id INTEGER, name VARCHAR ); INSERT INTO iceberg.lakehouse.test_table VALUES (1, 'Ahmed'), (2, 'Mohamed'); SELECT * FROM iceberg.lakehouse.test_table; ``` ### Running the Pipeline Access Airflow at `http://localhost:8080` (admin/admin), unpause `ecommerce_dag_pipeline`, and trigger it manually. Monitor the logs as the Custom `DbtOperator` passes context to `dbtRunner`. https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" /> ## 🧠 Engineering Highlights * **Git-for-Data Isolation:** By integrating Project Nessie, data engineers can test new models on a `dev` branch. `iceberg.nessie-catalog.ref=dev` routes Trino to a completely isolated state of the lakehouse, avoiding corruption of `main`[cite: 2]. * **Intelligent Resource Scaling:** During testing, the pipeline encountered `Maximum retry exceeded` errors due to RAM starvation when running too many Trino workers concurrently with Airflow and dbt[cite: 2]. By deliberately downscaling to **1 Trino worker** and limiting threads, local performance stabilized[cite: 2]. This proves that horizontal scaling is only effective when underlying hardware supports it[cite: 2]. * **Custom Pythonic `DbtOperator`:** Instead of running messy `BashOperator` scripts, a custom operator was built using `dbtRunner`[cite: 2]. It automatically handles checking for project directories, generating dynamic logs directories, parsing command arguments, and natively failing the Airflow task if the Python invocation fails[cite: 2]. * **Complete Lakehouse Decoupling:** Airflow orchestrates, dbt transforms, Trino queries, Iceberg abstracts the tables, and MinIO stores the files[cite: 2]. Every layer is independently scalable and replaceable. https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" /> ## 🛠️ Troubleshooting ### 1. System Freeze / "Maximum retry exceeded" Error * **Symptom:** Pipeline fails midway, containers crash or freeze. * **Cause:** Host system is running out of RAM (OOM) due to multiple Trino workers and high dbt thread counts[cite: 2]. * **Fix:** Scale down Trino to 1 Worker in `docker-compose.yaml` and reduce dbt thread count in `profiles.yml` to 1 or 2[cite: 2]. Restart with `docker compose down -v` and `docker compose up -d`[cite: 2]. ### 2. Trino cannot connect to MinIO * **Symptom:** `S3Exception: Access Denied` or connection refused in Trino logs. * **Fix:** Ensure `s3.endpoint=http://minio:9000` is set and `s3.path-style-access=true` is enabled in `iceberg.properties`[cite: 2]. Do not use `localhost` inside container configurations[cite: 2]. ### 3. Missing Namespaces in Nessie * **Symptom:** MinIO bucket exists, but Trino throws a `Namespace not found` error. * **Fix:** Creating a bucket in MinIO does not automatically create Iceberg namespaces[cite: 2]. You must run `CREATE SCHEMA iceberg.;` in Trino first. https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" /> ## 🔐 Security * Ensure the `.env` file containing MinIO root user/password and Postgres credentials is added to `.gitignore` and never committed to version control. * The current configuration (e.g., MinIO user `minioadmin`) is for **development purposes only**. * In a production deployment, inject credentials at runtime using secrets management tools (e.g., HashiCorp Vault, AWS Secrets Manager) instead of hardcoding them into Docker Compose environments. https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/aqua.png" width="100%" /> ## 🗺️ Roadmap **Current Status:** Active Development / Portfolio Project ### Planned Improvements - [ ] **Apache Spark**: Integrate Spark for heavy ML feature processing. - [ ] **Streaming Ingestion**: Introduce Apache Kafka / Redpanda for real-time E-commerce events. - [ ] **Data Quality Checkpoint**: Implement Soda or Great Expectations for robust assertions before publishing to Gold layer. - [ ] **Cloud Migration**: Provide Terraform configuration to deploy the stack to AWS (EKS, S3, RDS). - [ ] **CI/CD pipeline**: Add GitHub Actions for linting SQL (`sqlfluff`) and testing dbt models. --- **Author**: omaga333 | **Repository**: Distributed-Data-Lakehouse
