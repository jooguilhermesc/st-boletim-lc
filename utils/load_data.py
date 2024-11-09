from io import StringIO, BytesIO
from datetime import datetime   
from dotenv import load_dotenv
import pyarrow.parquet as pq
import pandas as pd
import requests
import boto3
import os
import io

load_dotenv()

class getDataFrame():

    def __init__(self, layer, final_layer, area, context, file_name, file_format, write_mode): #layer, final_layer, area, context, file_name, file_format, write_mode
        self.bucket_name = os.getenv("bucket_name")
        self.dt_load = datetime.today().strftime('%Y%m%d')
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.client = boto3.client('s3')

        self.layer = layer
        self.final_layer = final_layer
        self.area = area
        self.context = context
        self.file_name = file_name
        self.file_format = file_format
        self.write_mode = write_mode
        self.prefix = f"{self.final_layer}/{self.area}/{self.context}/{self.dt_load}/{self.file_name}"

    def listar_arquivos_parquet(self):
        arquivos_parquet = []
        paginator = self.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
            arquivos_parquet.extend([item['Key'] for item in page.get('Contents', []) if item['Key'].endswith('.parquet')])
        return arquivos_parquet

    def baixar_arquivo_parquet(self):

        arquivos_parquet = self.listar_arquivos_parquet()

        dfs = []
        for key in arquivos_parquet:
            # Baixa o arquivo Parquet para a memória
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            data = response['Body'].read()
            
            # Lê o arquivo Parquet a partir dos dados em memória
            df = pd.read_parquet(io.BytesIO(data))
            dfs.append(df)

        df_final = pd.concat(dfs, ignore_index=True)

        # Exibe as primeiras linhas do DataFrame resultante
        return df_final