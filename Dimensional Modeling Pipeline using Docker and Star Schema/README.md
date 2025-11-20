📂 ETL Data Pipeline Project: E-commerce Trends Analysis

🎯 Project Goal

The primary objective of this project was to construct a robust, containerized **Extract, Transform, Load (ETL)** pipeline using Docker, Python, and MySQL. This pipeline processes raw e-commerce data, performs data cleaning and transformation into a **Star Schema** (Dimensional Model), and loads the structured data into a persistent relational database (`database_1`).

---

🏗️ Architecture and File Structure

The solution uses Docker Compose to manage two core services: the database and the ETL engine.

Project Structure

This structure ensures all source code and data persistence are managed locally:

Data Engineering/ ├── docker-compose.yml # Defines the services and volumes ├── Dockerfile # Instructions for building the Python ETL app ├── requirements # Python dependencies (e.g., pandas, mysql-connector) ├── scripts/ # Container for ETL Python code │ └── ETL.py # The core ETL logic └── data/ # Local volume for persistence ├── shopping_trends.csv # The data source file └── mysql/ # Directory used to persist MySQL database files


Services Overview

* **`mysql_database`**: Persistent container running MySQL (MySQL 8.0) that acts as the **Destination Data Warehouse**.
* **`etl_pipeline`**: Container running the Python script (`ETL.py`) that executes the **ETL Engine**.

---

🛠️ Prerequisites

To run this project, you must have the following software installed:

* **Docker**
* **Docker Compose**

---

## 🚀 Setup and Execution

### 1. Build and Run the Pipeline

Execute the following command in the root directory (`Data Engineering/`):

```
docker-compose up --build
Result: The script will execute the full ETL process (E-T-L) and report success upon commitment.

2. Verification (Checking the Database)
After the ETL script reports success, you can connect to the MySQL container to verify the data load.

Connection Details:
Database: database_1

User: root

To connect to the database container's terminal:
# 1. Connect to the running MySQL service container
docker exec -it mysql_database mysql -u root -p
# (Enter your root password when prompted)

Verification queries:
USE database_1;
SELECT COUNT(*) FROM Fact_Purchase; -- Expected: 3900
SELECT COUNT(*) FROM Dim_Customer;  -- Expected: 3900
SELECT COUNT(*) FROM Dim_Item;      -- Expected: 25
⚙️ Data Model (Star Schema)
The ETL process transforms the flat data into a normalized Star Schema:

Fact Table (Fact_Purchase): Stores transactional metrics and foreign keys.

Measures: purchase_amount_usd, review_rating, previous_purchases.

Dimension Table (Dim_Customer): Stores unique customer details.

Attributes: age, gender, location, subscription_status, frequency_of_purchases.

Dimension Table (Dim_Item): Stores unique item attributes.

Attributes: item_name, category, size, color, season.

🛑 Cleanup
To stop and remove the running containers (required when finished to free up resources), run this command in the project directory:
docker-compose down
