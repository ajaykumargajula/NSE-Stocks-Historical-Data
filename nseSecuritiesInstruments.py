import os
from google.cloud import storage
import pandas as pd
import requests


def get_data(url):
    '''
    Function will check the response of URL passed and on status code 200, we scarp the data from URL and save in DataFrame format
    '''
    req= requests.get(url).status_code
    if req == 200:
        df = pd.read_csv(url)
        return df
    else:
        print('we are not able to access the url: {}'.format(url))

# NSE url to get list of Securities under Equity segment
equity_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
# Kite (Zerodha) URL to get instrument tokens for Security symbols. This is required to pull the data from Zerodha
kite_inst_url = "https://api.kite.trade/instruments"

nse_securities = get_data(equity_url)
kite_instruments = get_data(kite_inst_url)

'''
chunk of code to upload data to Google cloud storage buckets
# Only need this if you're running this code locally.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/your_GCP_creds/credentials.json'
client = storage.Client()
bucket = client.get_bucket('<Google Cloud Bucket name>')
bucket.blob('symbols/nse_securities.csv').upload_from_string(nse_securities.to_csv(), 'text/csv')
bucket.blob('symbols/kite_instruments.csv').upload_from_string(kite_instruments.to_csv(), 'text/csv')'''

#save data to local 
nse_securities.to_csv('nse_securities.csv')
kite_instruments.to_csv('kite_instruments.csv')
