import pandas as pd
import json
import os
import logging

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("transformation.log"),
        logging.StreamHandler()
    ]
)

class YouTubeTransformer:
    def __init__(self, raw_path="data/raw", processed_path="data/processed"):
        self.raw_path = raw_path
        self.processed_path = processed_path
        os.makedirs(self.processed_path, exist_ok=True)


    def parse_file(self, filename):
        """Parses a file based on its extension."""
        file_path = os.path.join(self.raw_path, filename)
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist.")

        ext = os.path.splitext(filename)[1].lower()
        
        try:
            if ext == '.json':
                with open(file_path, 'r') as f:
                    return json.load(f)
            elif ext == '.csv':
                return pd.read_csv(file_path)
            else:
                raise ValueError(f"Unsupported file type: {ext}")
        except Exception as e:
            logging.error(f"Error parsing {filename}: {e}")
            raise

    def transform(self):
        try:
            logging.info("Starting transformation process...")

            # Parse JSON for categories
            categories_data = self.parse_file("GB_category_id.json")
            category_mapping = {
                item['id']: item['snippet']['title'] 
                for item in categories_data['items']
            }
            logging.info(f"Extracted {len(category_mapping)} categories from JSON.")

            # Parse CSV for videos
            df = self.parse_file("GBvideos.csv")
            logging.info(f"Loaded {len(df)} rows from CSV.")

            # Data Cleaning & Mapping
            df['category_id'] = df['category_id'].astype(str)
            df['category_title'] = df['category_id'].map(category_mapping)

            # Validation check
            missing = df['category_title'].isnull().sum()
            if missing > 0:
                logging.warning(f"{missing} rows have unmapped categories.")
            else:
                logging.info("All categories mapped successfully.")

            # Formatting Dates
            df['trending_date'] = pd.to_datetime(df['trending_date'], format='%y.%d.%m')
            df['publish_time'] = pd.to_datetime(df['publish_time'])
            logging.info("Date columns formatted successfully.")

            # Save Result
            output_file = os.path.join(self.processed_path, "GB_videos_cleaned.csv")
            df.to_csv(output_file, index=False)
            logging.info(f"Transformation complete! File saved to: {output_file}")

        except FileNotFoundError as e:
            logging.error(f"File missing error: {e}")
        except Exception as e:
            logging.critical(f"Unexpected transformation error: {e}")

if __name__ == "__main__":
    transformer = YouTubeTransformer()
    transformer.transform()