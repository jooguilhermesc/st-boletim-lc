from utils.load_data import getDataFrame
from datetime import datetime
from st_keyup import st_keyup
import streamlit as st
import time

dt_load = datetime.today().strftime('%Y%m%d')
layer = "raw"
final_layer = "business"
area = "tcu"
context = "boletim-informativo-lc"
file_name = "boletim-informativo-lc"
file_format = "parquet"
write_mode = "overwrite"
data_from_s3 = getDataFrame(layer, final_layer, area, context, file_name, file_format, write_mode)
df = data_from_s3.baixar_arquivo_parquet()

@st.cache_data
def get_data(df):
    print(df)
    return df

with st.spinner('Carregando lista de processos monitorados...'):
    time.sleep(1)

df = get_data(df)

st.title("Boletim Informativo TCU - Licitações e Contratos")

col1, col2, col3 = st.columns(3)

filtered = df

relator = col1.multiselect(
    'Relator',
    filtered["relator_acordao"].unique())

colegiado = col2.multiselect(
    'Colegiado',
    filtered["colegiado_acordao"].unique())

tipo = col3.multiselect(
    'Tipo',
    filtered["tipo_acordao"].unique())

busca = st_keyup("Faça uma busca na base de acórdãos", key="0")

# Filtro de 'relator_acordao'
if relator:
    filtered = filtered[filtered['relator_acordao'].isin(relator)]

# Filtro de 'colegiado_acordao'
if colegiado:
    filtered = filtered[filtered['colegiado_acordao'].isin(colegiado)]

# Filtro de 'tipo_acordao'
if tipo:
    filtered = filtered[filtered['tipo_acordao'].isin(tipo)]

if busca:
    filtered = filtered[filtered.infos_acordao.str.lower().str.contains(busca.lower(),na=False)]


st.dataframe(filtered,column_config={
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