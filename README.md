# Austin Bike Sharing ETL Project

## Project Overview

This project implements an ETL pipeline to process historical bike sharing data from Austin (2013–present). The goal is to enable stakeholders to make informed decisions on bike usage patterns, station performance, demand distribution, and vehicle/subscription trends.

The pipeline extracts raw trip data, transforms it to calculate useful metrics (e.g., trips per kiosk, trips per hour, top stations, subscription and bike type analysis), and loads the results into a format suitable for visualization and analysis.

Source datasets are publicly available:

- **Bike Trips Data:** [Austin MetroBike Trips](https://data.austintexas.gov/Transportation-and-Mobility/Austin-MetroBike-Trips/tyfh-5r8s/about_data)
- **Kiosk Locations:** [Austin MetroBike Kiosk Locations](https://data.austintexas.gov/Transportation-and-Mobility/Austin-MetroBike-Kiosk-Locations/qd73-bsdg/about_data)

---

## Stakeholder Requirements

- **Trips by Start Kiosk:** Total trips per kiosk aggregated by day of week, month, and year.
- **Demand Distribution Over Time:** Hourly trips segmented by day of week, month, and year.
- **Geospatial Analysis of Stations:** Identify and visualize top 5 kiosks by number of trips per day of week and month.
- **Trips by Subscription Type:** Total trips made by each membership/pass type.
- **Trips by Vehicle Type:** Total trips by bike type (Classic vs. Electric).

---

## Proposed Data Warehouse

### 📌 Schematic Design

[Conceptual Design](./images/AustinTripsCoceptualDesign.jpg)

### 📌 Logical Design

[Logical Design](./images/AustinTripsLogicalDesign.jpg)
[Drawn on DBDiagram](https://dbdiagram.io/)

## Project Architecture

Below is a high-level architecture of the ETL pipeline:

This diagram shows the technical workflow:

- **Data Acquisition:** Automated via `run_etl.sh`, raw datasets are loaded into `data/raw`.
- **ETL Processing:** `etl_main.py` powered by Apache Spark distributed architecture loads the raw data, performs feature engineering, extracts dimensions, and computes measures.
- **Data Storage:** Fact and dimension tables are written into a PostgreSQL database.
- **Visualization:** Grafana dashboards are created for interactive filtering and exploration.
- **Containerization:** The entire project is containerized using Docker with three services: Spark, PostgreSQL, and Grafana.

---

## ETL Implementation Instructions

### 1️⃣ Prepare Jars

```bash
mkdir -p ./jars
wget -O ./jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
```

### 2️⃣ Make ETL Script Executable

```bash
chmod +x run_etl.sh
```

### 3️⃣ Build Docker Images

```bash
docker compose build
```

### 4️⃣ Start Docker Containers

```bash
docker compose up
```

### 5️⃣ Run ETL Pipeline

```bash
docker compose run --rm spark bash -c "/app/etl/run_etl.sh"
```

### 6️⃣ Open Grafana Container

Grafana can be accessed through your browser at `http://localhost:3000` (default credentials: `admin` / `admin`).

---

