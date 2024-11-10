from utils.load_data import getData
from datetime import datetime
from st_keyup import st_keyup
import streamlit as st
import pandas as pd
import time

# Define a data de carregamento no formato YYYYMMDD
dt_load = datetime.today().strftime('%Y%m%d')

# Parâmetros de configuração para acessar os dados no S3
layer = "raw"
final_layer = "business"
area = "tcu"
context = "boletim-informativo-lc"
file_name = "boletim-informativo-lc"
file_format = "parquet"
write_mode = "overwrite"

# Instancia a classe getData e baixa os dados do S3 em um DataFrame
try:
    data_from_s3 = getData(layer, final_layer, area, context, file_name, file_format, write_mode)
    df = data_from_s3.download_parquet_files()
except ValueError:
    df = pd.DataFrame([{"titulo_acordao":"Sem título",
                        "colegiado_acordao":"Desconhecido",
                        "enunciado_acordao":"Desconhecido",
                        "numero_acordao":"Desconhecido",
                        "infos_acordao":"",
                        "tipo_acordao":"Desconhecido",
                        "relator_acordao":"Desconhecido",
                        "ano_acordao":"1900",
                        "tipo_relator_acordao":"Desconhecido"}])  # Cria um DataFrame vazio em caso de erro
    print("")

@st.cache_data
def get_data(df):
    """
    Cacheia o DataFrame para evitar recarregamentos desnecessários durante a interação do usuário.

    Parâmetros:
        df (pandas.DataFrame): DataFrame contendo os dados baixados.

    Retorna:
        pandas.DataFrame: DataFrame cacheado para uso no aplicativo Streamlit.
    """
    return df

# Exibe uma mensagem de carregamento enquanto os dados são processados
with st.spinner('Carregando lista de processos monitorados...'):
    time.sleep(1)  # Simula o tempo de carregamento para feedback ao usuário

# Carrega o DataFrame cacheado
df = get_data(df)

# Título principal do aplicativo Streamlit
st.title("Boletim Informativo TCU - Licitações e Contratos")

# Criação de três colunas para filtros de dados
col1, col2, col3 = st.columns(3)

# Inicializa o DataFrame que será filtrado de acordo com as seleções do usuário
filtered = df

# Filtro multisseleção para 'Relator' dos acórdãos
relator = col1.multiselect(
    'Relator',
    filtered["relator_acordao"].unique())

# Filtro multisseleção para 'Colegiado' dos acórdãos
colegiado = col2.multiselect(
    'Colegiado',
    filtered["colegiado_acordao"].unique())

# Filtro multisseleção para 'Tipo' dos acórdãos
tipo = col3.multiselect(
    'Tipo',
    filtered["tipo_acordao"].unique())

# Campo de busca para localizar palavras-chave no conteúdo dos acórdãos
busca = st_keyup("Faça uma busca na base de acórdãos", key="0")

# Aplica o filtro 'Relator' se uma seleção for feita
if relator:
    filtered = filtered[filtered['relator_acordao'].isin(relator)]

# Aplica o filtro 'Colegiado' se uma seleção for feita
if colegiado:
    filtered = filtered[filtered['colegiado_acordao'].isin(colegiado)]

# Aplica o filtro 'Tipo' se uma seleção for feita
if tipo:
    filtered = filtered[filtered['tipo_acordao'].isin(tipo)]

# Aplica o filtro de busca no conteúdo dos acórdãos, ignorando maiúsculas e minúsculas
if busca:
    filtered = filtered[filtered.infos_acordao.str.lower().str.contains(busca.lower(), na=False)]

# Exibe o DataFrame filtrado com configuração de colunas personalizada
st.dataframe(filtered, column_config={
    "titulo_acordao": st.column_config.TextColumn(
        "Título do Acordão",
        help="Título do Acordão",
        width="medium",
        required=True,
    ),
    "colegiado_acordao": st.column_config.TextColumn(
        "Colegiado",
        width="medium",
        required=True,
    ),
    "enunciado_acordao": st.column_config.TextColumn(
        "Enunciado",
        width="medium",
        required=True,
    ),
    "numero_acordao": st.column_config.TextColumn(
        "Número",
        width="medium",
        required=True,
    ),
    "infos_acordao": st.column_config.TextColumn(
        "Conteúdo do Acórdão",
        width="medium",
        required=True,
    ),
    "tipo_acordao": st.column_config.TextColumn(
        "Tipo do Acordão",
        width="medium",
        required=True,
    ),
    "relator_acordao": st.column_config.TextColumn(
        "Relator",
        width="medium",
        required=True,
    ),
    "ano_acordao": st.column_config.TextColumn(
        "Ano do Acórdão",
        width="medium",
        required=True,
    ),
    "tipo_relator_acordao": st.column_config.TextColumn(
        "Ministro",
        width="medium",
        required=True,
    ),
})