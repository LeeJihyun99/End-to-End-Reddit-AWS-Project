import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from etls.youtube_etl import YoutubeETL


# Configuration
api_key = 'AIzaSyAhpasuDbRxIk2-7FSDFLsQv8SZkrYe2L4'
keyword = 'Korea'
maxResults = 50
publishedAfter = '2024-12-01T00:00:00Z'
publishedBefore = '2024-12-15T00:00:00Z'
etl_pipeline = YoutubeETL(api_key)


def youtube_pipeline():
    """Run the YouTube ETL pipeline."""
    # Connect to YouTube API
    instance = etl_pipeline.connect_youtube()
    
    # Extract data
    response = etl_pipeline.extract(instance, keyword, maxResults, publishedAfter, publishedBefore)
    
    # Transformation step (add logic later)
    # Example: df_transformed = transform(response)
    
    # Load step (add logic later)
    # Example: load_data_to_csv(df_transformed, 'youtube_data.csv')

    return response

# Run the pipeline
response = youtube_pipeline()

# Print the response (for testing)
print(response)
