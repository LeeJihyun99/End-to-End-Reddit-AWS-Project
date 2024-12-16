import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from etls.youtube_etl import YoutubeETL


# Configuration
maxResults = 50
publishedAfter = '2024-12-01T00:00:00Z'
publishedBefore = '2024-12-15T00:00:00Z'
etl_pipeline = YoutubeETL(api_key=<your_google_api_key>, keyword = 'Korea', next_page_token=None, page_number=0)

raw_json_data = None
response = None

def youtube_pipeline():
    """Run the YouTube ETL pipeline."""
    # Connect to YouTube API
    instance = etl_pipeline.connect_youtube()
    
    print("extracting through youtube data api")

    # Extract data
    while etl_pipeline.page_number <= 10:
        response = etl_pipeline.extract(instance,  maxResults, publishedAfter, publishedBefore)
        
        if response is not None:
            print("extracted successfully")

        raw_data = etl_pipeline.convert_to_json(response)

        if raw_data is not None:
            print(f"converted into JSON and saved it into raw_data folder: {etl_pipeline.keyword} for page {etl_pipeline.page_number}")     

        response = None
        raw_data = None

    # Transformation step (add logic later)
    # Example: df_transformed = transform(response)
    
    # Load step (add logic later)
    # Example: load_data_to_csv(df_transformed, 'youtube_data.csv')

# Run the pipeline
youtube_pipeline()



