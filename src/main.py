# import das minhas bibliotecas
import yfinance as yf
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

# Denifir uma sessão insegura foi necessário por conta de um erro do requests
unsafe_session = requests.session()
unsafe_session.verify = False

# Serve para desligar os avisos de erro de certificação SSL
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# importar as minhas variáveis de ambiente
commodities = ['CL=F', 'GC=F', 'SI=F']

DB_HOST=os.getenv('DB_HOST_PROD')
DB_PORT=os.getenv('DB_PORT_PROD')
DB_NAME=os.getenv('DB_NAME_PROD')
DB_USER=os.getenv('DB_USER_PROD')
DB_PASS=os.getenv('DB_PASS_PROD')
DB_SCHEMA=os.getenv('DB_SCHEMA_PROD')
DB_THREADS=os.getenv('DB_THREADS_PROD')
DB_TYPE=os.getenv('DB_TYPE_PROD')
DBT_PROFILES_DIR=os.getenv('DBT_PROFILES_DIR')

DB_URL=f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(DB_URL)

def buscar_dados_commodities(simbolo:str, periodo='5d', intervalo='1d') -> pd.DataFrame:
    ticker = yf.Ticker(simbolo, session=unsafe_session)
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados

def buscar_todos_dados_commodities(commodities:list) -> pd.DataFrame:
    todos_dados = []
    for simbolo in commodities:
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

def salvar_no_postgres(df:pd.DataFrame, schema='public'):
    df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema)

if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_commodities(commodities)
    salvar_no_postgres(dados_concatenados)
    print('Dados salvos com sucesso!')

# pegar a cotação dos meus ativos

# concatenar os meus ativos (1..2...3) -> (1)

# salvar no banco de dados