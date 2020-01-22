#!/bin/env python
import FinanceDataReader as fdr
import pandas as pd
# from pandas import Series, DataFrame
import sqlite3

conn = sqlite3.connect('./db/finance.db')

"""
# 종목코드 받아오기
df = fdr.StockListing('KRX', if_exists="replace")
df.to_sql('KRX', conn)
df = fdr.StockListing('NASDAQ', if_exists="replace")
df.to_sql('NASDAQ', conn)
df = fdr.StockListing('NYSE', if_exists="replace")
df.to_sql('NYSE', conn)
df = fdr.StockListing('AMEX', if_exists="replace")
df.to_sql('AMEX', conn)
df = fdr.StockListing('SP500', if_exists="replace")
df.to_sql('SP500', conn)

# ETF 종목코드 받아오기, 한국과 미국
# https://github.com/FinanceData/FinanceDataReader/wiki/Release-Note-0.8.0
df = fdr.EtfListing('KR', if_exists="replace")
df.to_sql('ETF_KR', con)
df = fdr.EtfListing('US', if_exists="replace")
df.to_sql('ETF_US', con)
"""

#  FinanceDataReader가 지원하는 1996년부터 모든 거래 데이터 받아오기
df = pd.read_sql("select * from KRX", con=conn, index_col='index')

"""
for Sym in df.Symbol:
    each_stock = fdr.DataReader(Sym, '1996-01-01', '2000-12-30', country='KR')
    each_stock.to_sql(Sym, conn, if_exists='append')

df = pd.read_sql("select * from KRX", con, index_col='index')

for Sym in df.Symbol:
    each_stock = fdr.DataReader(Sym, '2001-01-01', '2009-12-30', country='KR')
    each_stock.to_sql(Sym, conn, if_exists='append')

    df = pd.read_sql("select * from KRX", con, index_col='index')

for Sym in df.Symbol:
    each_stock = fdr.DataReader(Sym, '2010-01-01', '2019-12-30', country='KR')
    each_stock.to_sql(Sym, conn, if_exists='append')
"""

for Sym in df.Symbol:
    each_stock = fdr.DataReader(Sym, '2020-01-08', '2020-01-15', country='KR')
    each_stock.to_sql(Sym, conn, if_exists='append')

"""
# 모든 종목 테이블 드랍
for Sym in df.Symbol:
    dropTableStatement = "DROP TABLE \'" + Sym + "\'"
    print(dropTableStatement)
    conn.execute(dropTableStatement)
"""

conn.close()
