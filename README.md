The project uses a normalized relational design to reduce data redundancy:

1. categories table: Stores unique YouTube category IDs and their human-readable titles.
2. videos table: Stores daily trending snapshots with a Foreign Key link to the categories. Includes a unique constraint on (video_id, trending_date).

🛠 Setup & Installation
1. Prerequisites
  Python 3.10+
  PostgreSQL installed and running.
  A Kaggle account and API Token (kaggle.json).

2. Configure Kaggle API
   Place your kaggle.json inside a config/ folder at the root of the project
   Ensure permissions are restricted (macOS/Linux): chmod 600 config/kaggle.json

3. Database Configuration
   The pipeline defaults to user vineetsinha. If your local Postgres setup differs in .env

4. Install requirements
   pip install -r requirements.txt

📑 How to Run
  1. Fetch Data: Downloads raw files from Kaggle.
     python src/fetch_data.py

  2. Transform Data: Cleans and standardizes dates and categories.
     python src/transform_data.py

  3. Load to Database: Ingests data into PostgreSQL with upsert logic.
     python src/load_to_db.py

📈 Monitoring (Check the generated log files for execution details)

  1. data_fetch.log: Tracks Kaggle API downloads.
  2. transformation.log: Tracks data cleaning and mapping issues.
  3. ingestion.log: Tracks database record counts and conflicts.

