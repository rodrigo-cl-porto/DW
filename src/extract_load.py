# import
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import certifi
import os

# importar as minhas variáveis de ambiente
commodities = ['CL=F', 'GC=F', 'SI=F']

def buscar_dados_commodities(simbolo:str, periodo='5d', intervalo='1d'):
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados

def buscar_todos_dados_commodities(commodities):
    todos_dados = []
    for simbolo in commodities:
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)
    return pd.concat()

if __name__ == "__main__":
    #dados_concatenados = buscar_todos_dados_commodities(commodities)
    #print(dados_concatenados)
    print(certifi.where())

# pegar a cotação dos meus ativos

# concatenar os meus ativos (1..2...3) -> (1)

# salvar no banco de dados