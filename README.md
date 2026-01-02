 # ⚽Football Manager 24 - Data Warehouse and Analystics Project

Hola! 
Welcome to my **Football Manager 24 - Data Warehouse and Analystics Project** repository!
I took up this project to showcase my Data Engineering, Warehousing and Analystics capacity while also mixing a little pleasure with a new club in the Football Manager 24 game for PC. 

## 🏗️ Architecture & Methodology

The solution implements a **Medallion Architecture** (Bronze, Silver, Gold) within Microsoft SQL Server to ensure data quality and historization.

### 1. Data Ingestion & OCR (Python)
**Goal:** Extract data not natively exportable by the game.
* **Challenge:** "Average Possession" stats are only visible on GUI dashboards and cannot be exported to CSV.
* **Solution:** Developed a Python pipeline using `EasyOCR`, `PIL`, and `Pandas` to:
    * Concatenate multiple screenshot images into a single array.
    * Perform Optical Character Recognition (OCR) to extract text.
    * Clean and parse the output into a structured CSV dataset for ingestion.

### 2. The Data Warehouse (SQL Server)
The database `FiorentinaDW` is organized into three specific schemas:

#### 🥉 Bronze Layer (Raw)
* Acts as the landing zone for raw CSV data.
* Tables include `fmdata_team_players`, `fmdata_manager_data`, and `fmdata_possession_data`.
* Data is ingested "as-is" to preserve the original state.

#### 🥈 Silver Layer (Transformation & Historization)
* **Data Cleaning:** Standardizes strings (e.g., wage formats like `£15.5K/p/w` -> `15500`), removes generic suffixes (" - Pick Player"), and casts data types.
* **SCD Type 2 (Slowly Changing Dimensions):** Implemented via the `silver.load_silver` Stored Procedure.
    * Tracks changes in player attributes over time.
    * Uses columns `dwh_current_validity` and `dwh_cd_valid_till` to expire old records and insert new active ones, enabling time-travel analysis of player development.

#### 🥇 Gold Layer (Reporting & Analytics)
* **Data Normilization:** Designed a robust Star Schema, organizing data into Fact tables and Dimension tables to optimize query performance while utilizing Data Normalization techniques that enabled advanced tactical analysis through structured data querying.
* **Business Logic:** Contains SQL Views optimized for end-user reporting.
* **Aggregations:** Calculates complex metrics (e.g., "Progressive Rate per Pass", "Weighted Defensive Actions") to answer specific scouting questions.

***

## 😁About me
Hi there! My name is **Olufeoluwa** but my friends call me **Medal**. I'm an up-and-coming IT professional and as you might have guessed, an avid football fan. Enjoy!!!
