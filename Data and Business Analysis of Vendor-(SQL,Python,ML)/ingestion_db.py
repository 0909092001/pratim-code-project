import pandas as pd
import os
import logging
from sqlalchemy import create_engine # pyright: ignore[reportMissingImports]
import time

# Setup logging
logging.basicConfig(filename='logs/ingestion_db.log', level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s', filemode='a')

def load_raw_data():
    engine = create_engine('sqlite:///inventory.db')
    start_time = time.time()
    logging.info("Ingestion started")
    
    for file in os.listdir('data/'):
        if '.csv' in file:
            filepath = os.path.join('data', file)
            df = pd.read_csv(filepath)
            table_name = file.replace('.csv', '')
            logging.info(f"Ingesting {file} into DB")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
    
    total_time = (time.time() - start_time) / 60
    logging.info(f"Ingestion complete. Total time: {total_time:.2f} minutes")

if __name__ == "__main__":
    load_raw_data()
