import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from etls.youtube_etl import YoutubeETL


# Configuration
maxResults = 50
publishedAfter = '2024-12-01T00:00:00Z'
publishedBefore = '2024-12-15T00:00:00Z'
etl_pipeline = YoutubeETL(api_key='AIzaSyBOOtd-JArcnY_jxwcRP_l150LDcp_Igq4', keyword = 'Korea')

raw_json_data = None

def youtube_pipeline():
    """Run the YouTube ETL pipeline."""
    # Connect to YouTube API
    instance = etl_pipeline.connect_youtube()
    
    print("extracting through youtube data api")
    # Extract data
    response = etl_pipeline.extract(instance,  maxResults, publishedAfter, publishedBefore)

    print("extracted successfully")

    raw_data = etl_pipeline.convert_to_json(response)

    print("converted into JSON and saved it into raw_data folder")

    # Transformation step (add logic later)
    # Example: df_transformed = transform(response)
    
    # Load step (add logic later)
    # Example: load_data_to_csv(df_transformed, 'youtube_data.csv')

    return raw_data

# Run the pipeline
raw_json_data = youtube_pipeline()

# Print the response (for testing)
print(raw_json_data)



