# 🧱 Brick E3 – Workflow Automation with Airflow

![Gitpod Badge](https://gitpod.io/button/open-in-gitpod.svg)

This brick teaches you how to automate spatial ETL workflows using [Apache Airflow](https://airflow.apache.org/) and the [Astronomer CLI](https://docs.astronomer.io/astro/cli/overview). You'll work with real, daily-updated datasets to build production-style DAGs that ingest, transform, tile, and publish both raster and vector spatial data.

---

## 🎯 Learning Goals

- Understand the core components of an Airflow DAG
- Use the Astronomer CLI to develop DAGs in a reproducible environment
- Automate spatial workflows for both raster and vector data
- Generate and publish PMTiles using open-source tools

---

## 🗂 Project Overview

You'll build **two DAGs** using public, frequently updated datasets:

### 1. 🏔️ SNODAS Raster Workflow
- Downloads daily snow cover rasters from NOAA
- Computes day-over-day snow change
- Converts to GeoTIFF
- Tiles to PMTiles
- Simulates upload to S3

### 2. 🌪️ NOAA Weather Warnings Workflow
- Downloads the current nationwide warnings shapefile
- Converts to GeoParquet
- Tiles as vector PMTiles
- Simulates upload to S3

---

## 🧰 Tools You'll Use

- Apache Airflow 2.7+ via Astronomer CLI
- Python (`rioxarray`, `geopandas`, `requests`)
- Tippecanoe + PMTiles CLI for tile generation
- Gitpod for browser-based development

---

## 🚀 Getting Started

1. **Open in Gitpod**  
   Click the Gitpod badge above or use this URL:  
   `https://gitpod.io/#https://github.com/YOUR_REPO_HERE/e3-workflow-automation`

2. **Wait for setup to finish**  
   Gitpod will:
   - Build the Docker image
   - Install requirements
   - Launch Airflow via `astro dev start`

3. **Open Airflow UI**  
   Navigate to the port 8080 preview to open Airflow. Default credentials:
```

Username: admin
Password: admin

```

4. **Edit the DAGs**  
Fill in any remaining TODOs inside these two DAGs:
- `dags/snodas_to_pmtiles.py`
- `dags/noaa_storms_to_pmtiles.py`

5. **Trigger your DAGs**  
Use the Airflow UI to trigger runs and inspect the output under `/data` and `/output`.

---

## 🧪 Submission Instructions

To complete this brick and earn your badge:

1. Make sure both DAGs run successfully and generate a `.pmtiles` file.
2. Commit your changes to your forked repo.
3. Copy and submit the proof code displayed in your GitHub Actions comment after all tests pass.
4. Submit that code via the [Circle lesson form](https://YOUR_CIRCLE_URL_HERE) to claim your badge.

---

## 🧠 Optional Challenges

- Add a third DAG to automate tile invalidation or metadata publishing
- Publish the `.pmtiles` file to a real S3 bucket or CDN
- Add Slack notifications to the end of the DAG

---

## 📜 Badge Metadata

This badge certifies that the recipient has successfully completed the E3 Brick in the Modern GIS series, demonstrating real-world proficiency in spatial workflow automation using Apache Airflow. Learners mastered building reproducible DAGs for raster and vector data, tiling outputs to PMTiles, and simulating cloud publishing for scalable geospatial applications.

---

## ✅ Certified Skills

**Airflow DAG Design**  
Structure, schedule, and test modular Airflow workflows using best practices.

**Raster Workflow Automation**  
Ingest SNODAS rasters, compute differences with `rioxarray`, and tile with `rio-tiler`.

**Vector Workflow Automation**  
Ingest shapefiles, convert to GeoParquet, and tile with `tippecanoe` and `pmtiles`.

**Command Line Integration**  
Use `gdal_translate`, `tippecanoe`, and `pmtiles` from within Python and Airflow tasks.

**Cloud Publishing (Simulated)**  
Simulate pushing PMTiles archives to cloud storage for public web use.