import os
import logging
import zipfile

# Config file has kaggle.json
current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, 'config')

os.environ['KAGGLE_CONFIG_DIR'] = config_path
from kaggle.api.kaggle_api_extended import KaggleApi

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_fetch.log"), # Saves logs to a file
        logging.StreamHandler()                # Also prints to terminal
    ]
)


def download_youtube_data():
    try:
        # Initialize and Authenticate
        api = KaggleApi()
        api.authenticate()
        logging.info("Kaggle API Authenticated successfully.")

        dataset = "datasnaek/youtube-new"
        target_dir = "data/raw"
        os.makedirs(target_dir, exist_ok=True)

        # Check if Dataset are available
        logging.info(f"Checking availability for dataset: {dataset}")
        try:
            available_files = api.dataset_list_files(dataset).files
            available_filenames = [str(f) for f in available_files]
        except Exception as e:
            logging.error(f"Dataset '{dataset}' not found or not accessible. Error: {e}")
            return

        files_to_download = ["GB_category_id.json", "GBvideos.csv"]

        for file_name in files_to_download:
            # Check if specific file exists in the dataset
            if file_name not in available_filenames:
                logging.warning(f"File '{file_name}' not found in dataset. Skipping...")
                continue

            logging.info(f"Downloading {file_name}...")
            api.dataset_download_file(dataset, file_name, path=target_dir)
            
            # Handling ZIP files
            zip_path = os.path.join(target_dir, f"{file_name}.zip")
            if os.path.exists(zip_path):
                logging.info(f"Extracting {zip_path}...")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(target_dir)
                os.remove(zip_path)
                logging.info(f"Successfully extracted and cleaned {file_name}")
            else:
                logging.info(f"Downloaded {file_name} (no extraction needed).")

        logging.info("Data fetching process completed successfully.")

    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    download_youtube_data()