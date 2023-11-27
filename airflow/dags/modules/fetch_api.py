import requests
import pandas as pd

class FetchApi():
    def __init__(self, url):
        self.url = url

    def get_data(self):
        response = requests.get(self.url)
        result   = response.json()['data']['content']
        df       = pd.json_normalize(result)
        return df