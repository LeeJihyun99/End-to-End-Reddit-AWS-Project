from googleapiclient.discovery import build
import json
import pandas as pd

class YoutubeETL:
    def __init__(self, api_key):
        self.api_key = api_key
        self.youtube_instance = self.connect_youtube()

    def connect_youtube(self):
        """Connect to the YouTube API and return an instance."""
        return build('youtube', 'v3', developerKey=self.api_key)

    def extract(self, youtube_instance, keyword, max_results, published_after, published_before):
        """Extract data from the YouTube API."""
        request = self.youtube_instance.search().list(
            q=keyword,
            order="relevance",
            part="snippet",
            maxResults=max_results,
            publishedAfter=published_after,
            publishedBefore=published_before
        )
        response = request.execute()
        return response

    # def transform(self, response):
    #     """Transform the API response into a structured DataFrame."""
    #     items = response.get('items', [])
    #     transformed_data = [{
    #         'title': item['snippet']['title'],
    #         'channel': item['snippet']['channelTitle'],
    #         'publishedAt': item['snippet']['publishedAt'],
    #         'description': item['snippet']['description']
    #     } for item in items]
    #     return pd.DataFrame(transformed_data)

    # def load_data_to_csv(self, dataframe, file_name):
    #     """Load the transformed data into a CSV file."""
    #     dataframe.to_csv(file_name, index=False)
    #     print(f"Data successfully saved to {file_name}")
