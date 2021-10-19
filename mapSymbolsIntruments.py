import sys
from google.cloud import storage
import pandas as pd
import requests
import io

def storageBucket(bucket,blob):
    blob = bucket.get_blob(nse_blob)
    data = blob.download_as_string()
    df = pd.read_csv(io.BytesIO(data))
    return df

def get_data(url):
    
    req= requests.get(url).status_code
    if req == 200:
        df = pd.read_csv(url)
        return df
    else:
        print('we are not able to access the url')
        

equity_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
kite_inst_url = "https://api.kite.trade/instruments"

nse_df = get_data(equity_url)
inst_df = get_data(kite_inst_url)

client = storage.Client()
bucket = client.get_bucket('nse-historical-data')

bucket.blob('symbols/nse_securities.csv').upload_from_string(nse_df.to_csv(), 'text/csv')
bucket.blob('symbols/kite_instruments.csv').upload_from_string(inst_df.to_csv(), 'text/csv')

nse_df = nse_df[nse_df[' SERIES']!='BZ'].reset_index(drop=True)

inst_df = inst_df[(inst_df['segment']=='NSE')&(inst_df['exchange']=='NSE')]
inst_df = inst_df[~inst_df['name'].isnull()].sort_values(by=['tradingsymbol']).reset_index(drop=True)
inst_df['tradingsymbol'] = inst_df['tradingsymbol'].str.split('-BE',expand=True)[0]

kite_dict = dict(zip(inst_df['tradingsymbol'],inst_df['instrument_token']))

nse_df['instrument'] = nse_df['SYMBOL'].map(kite_dict)
nse_df = nse_df[['SYMBOL', 'NAME OF COMPANY', ' DATE OF LISTING', 'instrument']]

bucket.blob('symbols/nseEquitySymbols.csv').upload_from_string(nse_df.to_csv(), 'text/csv')
