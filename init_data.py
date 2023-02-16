<<<<<<< HEAD
#!/bin/env python
from logging import log
=======
#!/usr/bin/env python3
import os
import sys
>>>>>>> e77a3b6 (init_data.py: 각 마켓 심볼 가져오기 기능 개선)
import FinanceDataReader as fdr
import pandas as pd
import sqlite3

DB_FOLDER = "db/"
if not os.path.isdir(DB_FOLDER):
    os.mkdir(DB_FOLDER)

conn = sqlite3.connect('./db/finance.db')

<<<<<<< HEAD
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

=======
if len(sys.argv) > 1:
    MARKETS = sys.argv
else:
    MARKETS = [ "KRX", "ETF/KR", "NASDAQ", "NYSE", "AMEX", "SP500" ]

# 종목코드 받아오기
def get_symbol(MARKET):
    df = fdr.StockListing(MARKET)

    if MARKET in "KRX":
        df.rename(columns = {'Code' : 'Symbol'}, inplace = True)
    
    if checkTableExists(conn, MARKET):
        insertTable(df, MARKET)
    else:
        createTable(df, MARKET)
        insertTable(df, MARKET)

def insertTable(df, MARKET):
    df.to_sql(MARKET, conn, if_exists='replace')

def createTable(df, MARKET):
    schema = pd.io.sql.get_schema(df, MARKET)
    df.to_sql(schema, conn, if_exists="replace")

def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM sqlite_master 
        WHERE name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False

MARKETS.pop(0)
for MARKET in MARKETS:
    get_symbol(MARKET)

"""

# ETF 종목코드 받아오기, 한국과 미국
# https://github.com/FinanceData/FinanceDataReader/wiki/Release-Note-0.8.0
df = fdr.StockListing("ETF/KR") 
df.to_sql('ETF_KR', conn, if_exists='replace')

df = fdr.EtfListing('US', if_exists="replace")
df.to_sql('ETF_US', con)

#  FinanceDataReader가 지원하는 1996년부터 모든 거래 데이터 받아오기
"""
"""
df = pd.read_sql("select * from NASDAQ", con=conn, index_col='index')

for Sym in df.Symbol:
    each_stock = fdr.DataReader(Sym)
    each_stock.to_sql(Sym, conn, if_exists='append')

"""
>>>>>>> e77a3b6 (init_data.py: 각 마켓 심볼 가져오기 기능 개선)
"""
# 모든 종목 테이블 드랍
for Sym in df.Code:
    dropTableStatement = "DROP TABLE \'" + Sym + "\'"
    print(dropTableStatement)
    conn.execute(dropTableStatement)
"""

conn.close()
