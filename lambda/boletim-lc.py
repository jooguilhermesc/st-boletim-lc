from io import StringIO, BytesIO
from datetime import datetime   
import pandas as pd
import requests
import boto3

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('thereal-slim-shady')
    client = boto3.client('s3')

    url = "https://sites.tcu.gov.br/jurisprudencia/arquivos/boletim-informativo-lc/boletim-informativo-lc.csv"
    key = "boletim-informativo-lc"
    dt_load = datetime.today().strftime('%Y%m%d')

    r = requests.get(url, allow_redirects=True)
    csv_content = BytesIO(r.content)
    df = pd.read_csv(csv_content, sep="|")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False,encoding='utf-8',sep="|")
    csv_buffer.getvalue()
    path = f"raw/{key}/{dt_load}/{key}.csv"
    print(f"Enviando para o bucket - {path} - {url}")
    print("+","-"*len(url),"+")
    bucket.put_object(Key=path, Body=csv_buffer.getvalue())

    return {
        "statusCode": 200,
        "body": "Arquivos enviados para o datalake com sucesso!"
    }