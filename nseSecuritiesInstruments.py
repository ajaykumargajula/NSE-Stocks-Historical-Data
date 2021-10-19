import sys
from google.cloud import storage
import pandas as pd
import requests


def get_data(url):
    
    req= requests.get(url).status_code
    if req == 200:
        df = pd.read_csv(url)
        return df
    else:
        print('we are not able to access the url')
        
equity_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
kite_inst_url = "https://api.kite.trade/instruments"

nse_securities = get_data(equity_url)
kite_instruments = get_data(kite_inst_url)

client = storage.Client()
bucket = client.get_bucket('nse-historical-data')
bucket.blob('symbols/nse_securities.csv').upload_from_string(nse_securities.to_csv(), 'text/csv')
bucket.blob('symbols/kite_instruments.csv').upload_from_string(kite_instruments.to_csv(), 'text/csv')