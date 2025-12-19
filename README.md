# ShopZada Analytics | End-to-End Data Warehouse Solution

> A centralized, modular, and scalable end-to-end data warehouse to unify business, customer, enterprise, marketing, and operations department data into a single, analytical, and consistent data warehouse system to allow for exectuive decision making.
---

## Company Background
**ShopZada** is a rapidly growing e-commerce platform that has expanded globally, now handling over half a million orders and two million line items from diverse product categories.

### The Problem
Before this implementation, the company struggled with:
* **Data Silos:** Business data was trapped in different departments such as business, customer, enterprise, marketing, and operations department
* **Data Volume:** Since ShopZada is rapidly growing, the volume of data getting collected has increased exponentially increased.
* **Inconsistency:** The different data sources have incosistent data types, formats, and schemas.

### The Solution
This Data Warehouse (DWH) project provides a scalable, automated pipeline that cleanses, validates, models, and delivers high-quality data for deep analytics and applications of business intelligence.

---

## Project Team & Roles
The following team members designed and maintained the system:

| Name | Role | Core Responsibilities |
| :--- | :--- | :--- |
| **Bercasio, Vincent Jon** | **Project Manager and Analytics Engineer** | Facilitated the project roadmap, ensuring key milestones are made, and facilitated key business questions and application of BI |
| **Lampa, Kenzo Nicolo** | **ETL Engineer and BI Developer** | Developed ETL pipelines, and developed working and interactive dashboards |
| **Reyes, Darylle Joshua** | **Orchestration Engineer and BI Developer** | Modeled and developed the workflow orchestration of the system, and developed working and interactive dashboards |
| **Sampao, Sebastian James** | **ETL Engineer and Database Architect** | Developed ETL pipelines, and designed, modeled, and developed the data warehouse schema and architecture (star schema). |
| **Untalan, Chaurell Eichen** | **Analytics Engineer and Database Architect** | Designed, modeled, and developed the data warehouse schema and architecture (star schema), and facilitated key business questions and application of BI. |

---

## Data Architecture
Our system follows a **The Data Stack** architecture:

1. **Extraction/Loading (EL):** Data is pulled from the different departments using Python's Pandas and Glob.
2. **Storage:** All data is centralized in PostgreSQL.
3. **Transformation:** Using Python + SQL, the development team applied the star schema:
   * **Staging Area:** Validated and clean data ready for inserting into the warehouse.
   * **Dimension Tables:** Descriptive data for the fact table.
   * **Fact Tables:** Measures metrics of the specified grain per row per product per user per staff per merchant per order.
4. **BI/Analytics:** Final insights are delivered via Tableau.

---

## Tech Stack
* **Orchestration:** Apache Airflow
* **Transformation:** Pandas, Numpy, and PostgreSQL
* **Language:** Python 3.11, PostgreSQL
* **Containerization:** Docker
* **Visualization and BI:** Tableau

---

## Getting Started

### 1. Prerequisites
* Python 3.11
* Docker Desktop
* Tableau
### 2. Installation
```bash
# Clone the repository
git clone https://github.com/Sebinate/dwh_finalproject_3DSA_group_group3.git

# Enter the project directory
cd dwh_finalproject_3DSA_group_group3

# Create a data folder, the data will be stored here
mkdir data

# Create a artifacts folder for machine learning files
mkdir artifacts

# Ensure that a .env file is present with the following data
DB_USER
DB_PASSWORD
DB_HOST
DB_PORT
DB_NAME
PROJECT_HOME = "Project path here with format //c/Path"

# Initialize the warehouse (ensures no downstream issues later on)
docker compose up -d warehouse_db

# Initialize the rest of the services
docker compose up --build

#Log in into airflow with the following credentials
User: admin@example.com
Password: admin