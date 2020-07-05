#!/bin/env python
from logging import log
import FinanceDataReader as fdr
import pandas as pd
import sqlite3

conn = sqlite3.connect('./db/finance.db')

# 종목코드 받아오기
df = fdr.StockListing('KRX')
df.to_sql('KRX', conn, if_exists='replace')
df = fdr.StockListing('NASDAQ')
df.to_sql('NASDAQ', conn, if_exists='replace')
df = fdr.StockListing('NYSE')
df.to_sql('NYSE', conn, if_exists='replace')
df = fdr.StockListing('AMEX')
df.to_sql('AMEX', conn, if_exists='replace')
df = fdr.StockListing('SP500')
df.to_sql('SP500', conn, if_exists='replace')

# ETF 종목코드 받아오기, 한국과 미국
# https://github.com/FinanceData/FinanceDataReader/wiki/Release-Note-0.8.0
df = fdr.EtfListing('KR')
df.to_sql('ETF_KR', conn, if_exists='replace')
df = fdr.EtfListing('US')
df.to_sql('ETF_US', conn, if_exists='replace')

# 일일 가격 변동 받아오기
def get_marketdata(market_name):
    print(market_name)
    df = pd.read_sql('select * from \"' + market_name + '\"', con=conn, index_col='index')
    for Sym in df.Symbol:
        print (Sym)
        each_stock = fdr.DataReader(Sym, '2020-01-02', '2020-07-03')
        print (each_stock)
        each_stock.to_sql(Sym, conn, if_exists='append')

# markets = [ "KRX", "NASDAQ", "NYSE", "AMEX", "SP500", "ETF_KR", "ETF_US" ]
markets = [ "KRX", "ETF_KR" ]

for market in markets:
    get_marketdata(market)

"""
# 모든 종목 테이블 드랍
for Sym in df.Symbol:
    dropTableStatement = "DROP TABLE \'" + Sym + "\'"
    print(dropTableStatement)
    conn.execute(dropTableStatement)
"""

conn.close()
