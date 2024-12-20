from googleapiclient.discovery import build
import json
import pandas as pd

class YoutubeETL:
    def __init__(self, api_key, keyword, next_page_token, page_number):
        self.api_key = api_key
        self.keyword = keyword
        self.next_page_token = next_page_token
        self.page_number = page_number

    def connect_youtube(self):
        """Connect to the YouTube API and return an instance."""
        youtube_instance = build('youtube', 'v3', developerKey=self.api_key)
        return youtube_instance

    def extract(self, youtube_instance, max_results, published_after, published_before):
        """Extract data from the YouTube API."""
        if self.page_number == 0:
            request = youtube_instance.search().list(
                q = self.keyword,
                order="relevance",
                part="snippet",
                maxResults=max_results,
                publishedAfter=published_after,
                publishedBefore=published_before
            )

        else:
            request = youtube_instance.search().list(
                q = self.keyword,
                order="relevance",
                part="snippet",
                maxResults=max_results,
                publishedAfter=published_after,
                publishedBefore=published_before,
                pageToken = self.next_page_token
            )

        response = request.execute()
        self.next_page_token = response["nextPageToken"]
        self.page_number += 1
        
        return response
    
    def convert_to_json(self, response):
        if response is None:
            print("response not received yet.")
        else:
            raw_data = json.dumps(response)

            with open(f"data/raw_data/{self.keyword}_{self.page_number}.json", "w") as file:   
                file.write(raw_data)

        return raw_data
    
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

