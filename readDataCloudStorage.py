from google.cloud import storage
import pandas as pd
import io
'''
Read CSV file from Google cloud Storage using python
'''
cs_bucket = '<Google cloud Storage bucket name>'
nse_blob = '<File Name>'

client = storage.Client()
bucket = client.get_bucket(cs_bucket)
blob = bucket.get_blob(nse_blob)
data = blob.download_as_string()

nse_df = pd.read_csv(io.BytesIO(data))
