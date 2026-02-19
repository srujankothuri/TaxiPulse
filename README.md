# 🚕 TaxiPulse — Real-Time NYC Taxi Analytics Engine

An end-to-end data engineering platform that ingests millions of NYC taxi trip records through both batch and real-time streaming pipelines, validates data quality at every stage, models data in a star schema warehouse, detects pricing anomalies automatically, and visualizes insights through interactive dashboards.

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED?logo=docker)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-017CEE?logo=apache-airflow)
![Kafka](https://img.shields.io/badge/Streaming-Kafka-231F20?logo=apache-kafka)
![PostgreSQL](https://img.shields.io/badge/Warehouse-PostgreSQL-4169E1?logo=postgresql)
![MinIO](https://img.shields.io/badge/Storage-MinIO-C72E49?logo=minio)

---

## 🔗 Live Demo

| Resource | Link |
|----------|------|
| 📊 Analytics Dashboard | [Looker Studio](#) |
| 🖥️ Monitoring Console | [Streamlit App](#) |
| 🎬 Pipeline Demo | [Video Walkthrough](#) |

---

## 🏗️ Architecture

```
NYC TLC Data ──┬── Batch Path (Airflow) ──┐
               │                          ├── MinIO (Bronze) ── Great Expectations
               └── Stream Path (Kafka) ───┘         │
                                                     ▼
                                            PostgreSQL (Silver → Gold)
                                                     │
                                          ┌──────────┼──────────┐
                                          ▼          ▼          ▼
                                     Star Schema  Anomaly    Streamlit
                                     Warehouse    Detection  Dashboard
                                                  + Alerts
```

<!-- TODO: Replace with full architecture diagram image -->
<!-- ![Architecture](docs/images/architecture.png) -->

---

## ✨ Key Features

- **Dual Ingestion**: Batch (Airflow) + Real-time streaming (Kafka) pipelines
- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **Automated Data Quality**: Great Expectations validation at every stage
- **Star Schema Warehouse**: Dimensional model in BigQuery (fact + dimension tables)
- **Anomaly Detection**: Z-score based fare spike detection with Slack alerts
- **Infrastructure as Code**: Full GCP setup via Terraform
- **Containerized**: Docker Compose for reproducible deployment
- **Interactive Dashboards**: Looker Studio + Streamlit monitoring console

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| **Orchestration** | Apache Airflow |
| **Streaming** | Apache Kafka |
| **Object Storage** | MinIO (S3-compatible) |
| **Data Warehouse** | PostgreSQL |
| **Data Quality** | Great Expectations |
| **Anomaly Detection** | Python (scipy, numpy) |
| **Alerting** | Slack Webhooks |
| **Containerization** | Docker + Docker Compose |
| **Visualization** | Streamlit |
| **Language** | Python 3.11+ |

---

## 📂 Project Structure

```
TaxiPulse/
├── airflow/                  # Airflow DAGs and configuration
│   ├── dags/
│   ├── plugins/
│   └── config/
├── ingestion/                # Data ingestion (batch + streaming)
│   ├── batch/
│   └── streaming/
├── transformations/          # Bronze → Silver → Gold
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── quality/                  # Great Expectations data quality
├── anomaly_detection/        # Anomaly detection + alerting
├── terraform/                # GCP infrastructure as code
├── streamlit_app/            # Monitoring dashboard app
├── docker/                   # Docker configuration
├── tests/                    # Unit and integration tests
├── config/                   # Settings and constants
├── docs/                     # Documentation and diagrams
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Docker & Docker Compose

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/TaxiPulse.git
cd TaxiPulse

# 2. Configure environment
cp .env.example .env

# 3. Start all services
docker-compose up -d

# 4. Access the services
#    Airflow UI:      http://localhost:8080 (admin/admin)
#    MinIO Console:   http://localhost:9001 (taxipulse/taxipulse123)
#    PostgreSQL:      localhost:5432 (taxipulse/taxipulse123)
```

---

## 📊 Data Model (Star Schema)

```
                    ┌──────────────┐
                    │ dim_datetime  │
                    └──────┬───────┘
                           │
┌──────────────────┐  ┌────┴─────┐  ┌──────────────────┐
│ dim_pickup_loc   ├──┤fact_trips├──┤ dim_dropoff_loc   │
└──────────────────┘  └──┬────┬──┘  └──────────────────┘
                         │    │
              ┌──────────┘    └──────────┐
              ▼                          ▼
     ┌────────────────┐       ┌──────────────────┐
     │ dim_payment    │       │ dim_rate_code     │
     └────────────────┘       └──────────────────┘
```

---

## 📈 Dashboards

<!-- TODO: Add screenshots -->
Coming soon...

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Srujan Kothuri**
- GitHub: [@srujankothuri](https://github.com/srujankothuri)
- LinkedIn: [srujan kothuri](https://www.linkedin.com/in/srujan-kothuri-2044ba250/)
