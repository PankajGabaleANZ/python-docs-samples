import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from google.cloud.storage.blob import Blob
from google.cloud import storage
url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

r = requests.get(url,timeout=2.5)
r_html = r.text
soup = BeautifulSoup(r_html, 'html.parser')
components_table = soup.find_all(id="constituents")
headers_html = soup.find_all("th")
df_columns=[item.text.rstrip("\n") for item in headers_html]

components_headers=df_columns[:9]
data_rows=components_table[0].find("tbody").find_all("tr")[1:]
rows=[]
for row in range(len(data_rows)):
    stock=list(filter(None,data_rows[row].text.split("\n")))
    rows.append(stock)

S_P_500_stocks=pd.DataFrame(rows,columns=components_headers)

S_P_500_stocks.drop("SEC filings",inplace=True,axis=1)
S_P_500_stocks.to_csv("SP500stocks.csv",index=False, encoding="utf-8")


os.environ['GOOGLE_APPLICATION_CREDENTIALS']="EY-FIn-POC-d73eb642560d.json"
storage_client = storage.Client()
bucket = storage_client.get_bucket("poc-bucket-ey-demo")
data=bucket.blob("SP500stocks.csv")
data.upload_from_filename(r"SP500stocks.csv")

i


