# 🚕 TaxiPulse — Real-Time NYC Taxi Analytics Engine

An end-to-end data engineering platform that ingests millions of NYC taxi trip records through both batch and real-time streaming pipelines, validates data quality at every stage, models data in a star schema warehouse, detects pricing anomalies automatically, and visualizes insights through interactive dashboards.

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)
![GCP](https://img.shields.io/badge/Cloud-GCP-4285F4?logo=google-cloud)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-017CEE?logo=apache-airflow)
![Kafka](https://img.shields.io/badge/Streaming-Kafka-231F20?logo=apache-kafka)
![BigQuery](https://img.shields.io/badge/Warehouse-BigQuery-669DF6?logo=google-cloud)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED?logo=docker)
![Terraform](https://img.shields.io/badge/IaC-Terraform-844FBA?logo=terraform)

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
               │                          ├── GCS (Bronze) ── Great Expectations
               └── Stream Path (Kafka) ───┘         │
                                                     ▼
                                            BigQuery (Silver → Gold)
                                                     │
                                          ┌──────────┼──────────┐
                                          ▼          ▼          ▼
                                     Star Schema  Anomaly    Looker +
                                     Warehouse    Detection  Streamlit
                                                  + Alerts   Dashboard
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
| **Cloud Storage** | Google Cloud Storage (GCS) |
| **Data Warehouse** | Google BigQuery |
| **Data Quality** | Great Expectations |
| **Transformations** | dbt / Python |
| **Anomaly Detection** | Python (scipy, numpy) |
| **Alerting** | Slack Webhooks |
| **IaC** | Terraform |
| **Containerization** | Docker + Docker Compose |
| **Visualization** | Looker Studio + Streamlit |
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
- GCP Account with billing enabled
- Terraform installed

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/TaxiPulse.git
cd TaxiPulse

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your GCP credentials and settings

# 5. Deploy infrastructure
cd terraform
terraform init
terraform apply

# 6. Run the pipeline
docker-compose up -d
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

**Venkata Srujan**
- GitHub: [@your-username](https://github.com/your-username)
- LinkedIn: [your-linkedin](https://linkedin.com/in/your-linkedin)