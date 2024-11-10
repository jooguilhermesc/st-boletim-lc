# Boletim Informativo TCU - Licitações e Contratos

Este projeto é uma aplicação em Streamlit que consome dados da camada `business` de um Data Lake hospedado na AWS S3. Ele exibe informações de acórdãos relacionados a licitações e contratos do Tribunal de Contas da União (TCU), permitindo que os usuários filtrem e pesquisem dados de forma interativa.

## Visão Geral do ETL

O processo de ETL (Extract, Transform, Load) é responsável por extrair, transformar e carregar os dados da jurisprudência do TCU no Data Lake. A estrutura do ETL é organizada nas camadas `raw`, `trusted` e `business`, para facilitar o processamento e organização dos dados.

### Etapas do Processo ETL

1. **Extract (Extração)**: Uma função AWS Lambda baixa o arquivo CSV do site do TCU e o armazena na camada `raw` do S3.
2. **Transform (Transformação)**: O arquivo é processado nas camadas `raw` e `trusted`, onde são aplicadas verificações de qualidade e formatações.
3. **Load (Carregamento)**: Os dados transformados são transferidos para a camada `trusted` e posteriormente disponibilizados na camada `business`, prontos para consumo pela aplicação.

## Estrutura do Data Lake

Os dados são organizados por camadas:
- **`raw`**: Armazena o dado bruto conforme baixado.
- **`trusted`**: Contém dados transformados e validados.
- **`business`**: Dados prontos para uso analítico, organizados e otimizados para consultas frequentes.

## Tecnologias Utilizadas

- **AWS Lambda**: Automatiza o download e armazenamento de dados no S3.
- **AWS S3**: Armazena as camadas de dados do Data Lake.
- **PySpark/Databricks**: Realiza a transformação e limpeza dos dados.
- **Streamlit**: Interface de usuário interativa para visualização dos dados.
