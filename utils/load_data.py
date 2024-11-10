from io import StringIO, BytesIO
from datetime import datetime   
from dotenv import load_dotenv
import pyarrow.parquet as pq
import pandas as pd
import requests
import boto3
import os
import io

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

class getData:
    """
    Classe para gerenciar e processar dados armazenados em arquivos Parquet no Amazon S3.
    
    A classe permite listar e baixar arquivos Parquet de uma camada S3 específica, organizando
    os dados em um DataFrame pandas.
    """

    def __init__(self, layer, final_layer, area, context, file_name, file_format, write_mode):
        """
        Inicializa a classe com informações do bucket S3, caminho do arquivo e parâmetros de configuração.

        Parâmetros:
            layer (str): Camada inicial do armazenamento S3.
            final_layer (str): Camada final do armazenamento S3.
            area (str): Área temática ou de aplicação dos dados.
            context (str): Contexto específico dos dados.
            file_name (str): Nome base do arquivo.
            file_format (str): Formato do arquivo, por exemplo, 'parquet'.
            write_mode (str): Modo de escrita para o arquivo, como 'overwrite' ou 'append'.
        """
        self.bucket_name = os.getenv("bucket_name")  # Nome do bucket S3, carregado de variáveis de ambiente
        self.dt_load = datetime.today().strftime('%Y%m%d')  # Data de carregamento no formato YYYYMMDD
        self.s3 = boto3.resource('s3')  # Recurso S3 do boto3
        self.bucket = self.s3.Bucket(self.bucket_name)  # Instância do bucket S3
        self.client = boto3.client('s3')  # Cliente S3 para interagir com o bucket
        self.layer = layer
        self.final_layer = final_layer
        self.area = area
        self.context = context
        self.file_name = file_name
        self.file_format = file_format
        self.write_mode = write_mode
        # Prefixo do caminho no S3, usado para listar e acessar arquivos
        self.prefix = f"{self.final_layer}/{self.area}/{self.context}/{self.dt_load}/{self.file_name}"

    def list_parquet_files(self):
        """
        Lista todos os arquivos Parquet em um diretório específico do bucket S3.

        Retorna:
            List[str]: Lista de caminhos dos arquivos Parquet encontrados no bucket S3.
        """
        arquivos_parquet = []
        paginator = self.client.get_paginator('list_objects_v2')
        
        # Pagina pelos objetos no bucket, filtrando arquivos com extensão '.parquet'
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
            arquivos_parquet.extend(
                [item['Key'] for item in page.get('Contents', []) if item['Key'].endswith('.parquet')]
            )
        return arquivos_parquet

    def download_parquet_files(self):
        """
        Baixa e concatena arquivos Parquet do bucket S3 em um único DataFrame pandas.

        Retorna:
            pandas.DataFrame: DataFrame contendo dados de todos os arquivos Parquet baixados.
        """
        arquivos_parquet = self.list_parquet_files()  # Lista os arquivos Parquet disponíveis

        dfs = []  # Lista para armazenar DataFrames individuais
        for key in arquivos_parquet:
            # Baixa cada arquivo Parquet e lê-o como DataFrame pandas
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            data = response['Body'].read()
            df = pd.read_parquet(io.BytesIO(data))
            dfs.append(df)

        # Concatena todos os DataFrames em um único DataFrame final
        df_final = pd.concat(dfs, ignore_index=True)
        return df_final
