import requests
import pandas as pd
import datetime
from datetime import datetime,timedelta

token1="3f89c22f1c6cc86d260cfee54cbb6877397378fc"

# SP=pd.read_csv(r"SP500stocks.csv")

ticker_symbols=list(["GOOG", "ANZBY", "IAUGF", "WOLWF"])
ticker_symbols.append("SPY")
end_date=str(datetime.now().date()-timedelta(days=1))


# CONfigure ths to EOM - MOnth
# Check for the companes from 2000 - For all stocks
# CHeck f 1990s will

data1=[]

for symbol in ticker_symbols:        
  url = "https://api.tiingo.com/tiingo/daily/{}/prices?startDate=2000-01-03&endDate={}&format=csv&token={}".format(symbol,end_date,token1)
  r = requests.get(url,timeout=10)
  rows=r.text.split("\n")[1:-1]
  df=pd.DataFrame(rows)
  print(url)
  df=df.iloc[:,0].str.split(",",13,expand=True)
  df["Symbol"]=symbol
  data1.append(df)

df_final=pd.concat(data1)

df_final.drop(df_final.iloc[:,6:13],axis=1,inplace=True)

df_final.to_csv(r"historicalSP_quotes.csv",index=False, encoding="utf-8")

import os
from google.cloud.storage.blob import Blob
from google.cloud import storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r"C:\\Users\\gabalpa\\Documents\\EY-FIn-POC-d73eb642560d.json"
storage_client = storage.Client()
bucket = storage_client.get_bucket("poc-bucket-ey-demo")
data=bucket.blob("historicalSP_quotes.csv")
data.upload_from_filename(r"historicalSP_quotes.csv")


def csvloader(data,context):
    from google.cloud import bigquery
    client = bigquery.Client()
    table_ref = client.dataset("csm").table("SPhistorical")
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition =     bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 1
    
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = "gs://csm_erat/Data/historicalSP_quotes.csv"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    destination_table = client.get_table(table_ref)