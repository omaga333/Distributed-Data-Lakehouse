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
