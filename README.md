أكيد، هقسم لك الـ README على أجزاء (رسايل) منفصلة عشان يكون أسهل في النسخ والتعديل، وميقطعش أو يلخبط في التنسيق. كمان ظبطت الروابط بتاعة الصور عشان تشتغل معاك مظبوط، ورتبت الجزء بتاع الـ Project Structure.

ده **الجزء الأول** (بيحتوي على المقدمة، الفهرس، والـ Architecture):

---

# 🏛️ Distributed Data Lakehouse

### Enterprise-Grade Lakehouse Architecture Powered by Trino, Apache Iceberg, Project Nessie, dbt Core & Apache Airflow

---

## 📖 Table of Contents

* [Executive Summary](https://www.google.com/search?q=%23-executive-summary)
* [Architecture & Core Concepts](https://www.google.com/search?q=%23%EF%B8%8F-distributed-data-lakehouse-architecture)
* [End-to-End Data Workflow](https://www.google.com/search?q=%23-end-to-end-data-workflow)
* [The Medallion Data Model](https://www.google.com/search?q=%23-the-medallion-data-model)
* [Dockerized Infrastructure & Networking](https://www.google.com/search?q=%23-dockerized-infrastructure--networking)
* [Service Endpoints](https://www.google.com/search?q=%23-service-endpoints)
* [Deep Component Breakdown](https://www.google.com/search?q=%23-deep-component-breakdown)
* [Project Structure](https://www.google.com/search?q=%23-project-structure)
* [Configuration](https://www.google.com/search?q=%23-configuration)
* [Quick Start & Deployment Guide](https://www.google.com/search?q=%23-quick-start)
* [Validation & Smoke Tests](https://www.google.com/search?q=%23-validation--smoke-tests)
* [Engineering Highlights](https://www.google.com/search?q=%23-engineering-highlights)
* [Troubleshooting](https://www.google.com/search?q=%23-troubleshooting)
* [Security](https://www.google.com/search?q=%23-security)
* [Roadmap](https://www.google.com/search?q=%23-roadmap)

---

## 📌 Executive Summary

The **Distributed Data Lakehouse** is an end-to-end analytical data platform designed to process high-volume E-commerce data efficiently. It completely decouples storage, metadata, compute, and orchestration, resolving the limitations of traditional Data Warehouses (inflexibility, high cost) and Data Lakes (data swamps, lack of ACID guarantees).

At its core, this project demonstrates a highly robust **ELT pipeline**:

* **Storage & Format**: Raw and processed data are stored in **MinIO** as highly optimized Parquet files managed by **Apache Iceberg** table formats.


* **Compute & Catalog**: **Trino** executes all distributed SQL queries against the data, resolving table states and branches (e.g., `main` vs `dev`) through the **Project Nessie** catalog.


* **Transformation & Orchestration**: **dbt Core** handles the Medallion transformation logic (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), meticulously orchestrated by an **Apache Airflow** DAG using a custom-built Python `DbtOperator`.



---

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
ده **الجزء التاني** (بيحتوي على الـ Workflow، تفاصيل الـ Medallion Model، الـ Docker Infrastructure، ونقاط الـ Endpoints والـ Components):

---

## 🔄 End-to-End Data Workflow

The data pipeline runs on a scheduled cadence (e.g., every 15 minutes) processing high volumes of raw E-commerce events.

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

---

## 📊 The Medallion Data Model

The lakehouse adopts the Medallion Architecture to progressively improve data structure, ensuring analytics dashboards only read optimized, heavily-aggregated data.

| Layer | Objective & Transformations | Examples from E-commerce Data |
| --- | --- | --- |
| **Bronze** 🥉 | Stores raw source data. Modifies timestamps, handles empty strings (`''` to `NULL`), and adds audit metadata (`ingested_at`, `source_system`).

 | `bronze_customer_events`, `bronze_inventory_snapshots`, `bronze_payment_transactions`, `bronze_support_tickets`<br> |
| **Silver** 🥈 | Data cleaning, standardizing, and applying business logic. Handles sessionization (grouping user clicks), risk profiling for payments, and stock categorizations (Out of Stock, Low Stock).

 | `silver_customer_sessions`, `silver_inventory_status`, `silver_payment_profiles`, `silver_support_metrics`<br> |
| **Gold** 🥇 | Business-ready data. Pre-calculates heavy metrics, KPIs, aggregations, and daily rollups. Ready for BI tools (Power BI, Tableau) to consume directly without processing lag.

 | `gold_daily_metrics`, `gold_customer_summary`, `gold_product_summary`<br> |

---

## 🐳 Dockerized Infrastructure & Networking

The environment is strictly containerized via Docker Compose. Understanding the networking routing is critical:

* **Host Networking:** For accessing UIs from your local browser (e.g., `localhost:9080` routes to Trino).
* **Container Networking:** For services communicating internally (e.g., Airflow accessing Trino via `trino-coordinator:8080`).



| Service | Role | Host Port | Container Port | Exposure | Notes |
| --- | --- | --- | --- | --- | --- |
| `postgres` | Airflow Metadata DB | - | `5432` | Internal | Stores Airflow DAG states and configurations.

 |
| `redis` | Celery Broker | - | `6379` | Internal | Message broker for Airflow CeleryExecutor.

 |
| `minio` | S3 Object Storage | `9000`, `9001` | `9000`, `9001` | **External** | API (9000), Console Browser (9001).

 |
| `nessie-catalog` | Iceberg Catalog | `19120` | `19120` | **External** | Branching/Versioning.

 |
| `trino-coordinator` | Query Coordinator | `9080` | `8080` | **External** | Host port mapped to 9080 to avoid Airflow's 8080 conflict.

 |
| `trino-worker-1` | Query Worker | - | `8080` | Internal | Executes Trino tasks.

 |
| `airflow-apiserver` | Airflow UI/API | `8080` | `8080` | **External** | Airflow 3.0.6.

 |
| `airflow-worker` | Task Executor | - | - | Internal | Runs the custom Python DbtOperator.

 |
| *(Airflow Core)* | Scheduler, Triggerer | - | - | Internal | Orchestration background daemon services.

 |

---

## 🔌 Service Endpoints

| Service | Host URL | Container URL | Purpose |
| --- | --- | --- | --- |
| **Airflow UI** | `http://localhost:8080` | `http://airflow-apiserver:8080` | Monitor DAG runs, task logs, and schedules.

 |
| **Trino UI** | `http://localhost:9080` | `http://trino-coordinator:8080` | View query execution plans and worker stats.

 |
| **MinIO API** | `http://localhost:9000` | `http://minio:9000` | S3 API Endpoint for Iceberg table file writes.

 |
| **MinIO Console** | `http://localhost:9001` | `http://minio:9001` | Object browser UI to inspect Parquet data files.

 |
| **Nessie Catalog** | `http://localhost:19120` | `http://nessie-catalog:19120` | Catalog REST API for querying metadata branches.

 |

---

## 🔍 Deep Component Breakdown

### 🌬️ Apache Airflow & Custom `DbtOperator`

Airflow acts strictly as an orchestrator ("When and how to execute") rather than performing data transformations itself. To bridge Airflow and dbt elegantly, a custom **`DbtOperator`** was developed in Python extending Airflow's `BaseOperator`. This class uses the `dbtRunner` API to execute dbt commands directly inside the Airflow worker memory, rather than relying on clumsy Bash subshells.

### 🛠️ dbt Core (Data Build Tool)

dbt handles all data transformations ("How the data is modified").

* **Tags & Execution**: The Airflow DAG triggers dbt models sequentially using tags (`dbt run --select tag:bronze`, then `tag:silver`, then `tag:gold`).


* **Testing**: Data quality assertions (`unique`, `not_null`) are defined in `schema.yml` and run against the Iceberg tables via `dbt test` to ensure referential integrity.



### ⚡ Trino (Query Engine)

Trino is the distributed SQL engine executing the actual queries ("Who executes the SQL"). It is connected to the Iceberg catalog via its `iceberg.properties` configuration. Trino evaluates predicates, performs data skipping, and reads/writes the Parquet files stored in MinIO.

### 🧊 Project Nessie & Apache Iceberg

* **Iceberg**: Manages the open table format metadata (Snapshots $\rightarrow$ Manifest Lists $\rightarrow$ Manifest Files $\rightarrow$ Parquet) allowing for data skipping, concurrent writes, and time travel.


* **Nessie**: Acts as "Git for Data". It allows Trino to point to different branches of the catalog (e.g., `main` vs `dev`). You can isolate development transformations in the `dev` branch without affecting production `main` data.



---

