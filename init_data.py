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
import argparse
import datetime

## 시간 설정
now = datetime.datetime.now()
TODAY = now.strftime('%Y-%m-%d')


## DB 디렉토리 확인 후 없으면 생성
DB_FOLDER = "db/"
if not os.path.isdir(DB_FOLDER):
    os.mkdir(DB_FOLDER)

## sqlite3 DB 연결, 없으면 파일 생성
conn = sqlite3.connect('./db/finance.db')

<<<<<<< HEAD
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
=======
## 인자 확인: 0는 실행파일 명, 1 이후가 인자
## 비어 있으면 전체 마켓 업데이트
>>>>>>> eab9008 (init_data.py: 주석 추가, 마켓 입력 버그 수정)
if len(sys.argv) > 1:
    MARKETS = sys.argv
    MARKETS.pop(0)
else:
    MARKETS = [ "ETF/KR", "KRX" ]
    # MARKETS = [ "ETF/KR", "KRX", "NASDAQ", "NYSE", "SP500" ]

################################################################################
## 함수 모음
## 종목코드 받아오기
## MARKET 에 대해 종목코드를 받아오고 DB 에 저장 -> 테이블 없으면 생성
def get_symbol(MARKET):
    df = fdr.StockListing(MARKET)

    ## 한국만 Symbol 이 아니라 Code 인데 호환성을 위해 Rename
    if MARKET in "KRX":
        df.rename(columns = {'Code' : 'Symbol'}, inplace = True)
    
    if checkTableExists(conn, MARKET):
        insertTable(df, MARKET)
    else:
        createTable(df, MARKET)
        insertTable(df, MARKET)

def insertTable(df, DB_TABLE):
    # df.to_sql(DB_TABLE, conn, if_exists='replace')
    print("insert Table")

def createTable(df, DB_TABLE):
    schema = pd.io.sql.get_schema(df, DB_TABLE)
    # df.to_sql(schema, conn, if_exists="replace")
    print("create Table")

## 개별 종목 가격 정보 보유 여부 확인
## 테이블 존재하는지 체크 - 존재 하면 날짜 체크
## 날짜 비교 해서 갖고 있는 날짜는 dataframe 에서 drop, 보유하지 않은 날짜만 insert
def updatePrice(SYMBOL):
    df = pd.read_sql('select Date from \"' + SYMBOL + '\"', con=conn)
    each_stock = fdr.DataReader(SYMBOL, df['Date'].iloc[-1], TODAY)
    print(each_stock)



## 테이블 존재 유무 확인
## sqlite3 마스터 테이블에 MARKET 명으로 질의: 존재하면 True / 없으면 False Return
def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM sqlite_master 
        WHERE name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        #dbcur.close()
        return True

    #dbcur.close()
    return False

## 가격 업데이트
## 옵션 -p 추가 시 실행(예정)
def getPrice(MARKET):
    print(MARKET)
    df = pd.read_sql('select * from \"' + MARKET + '\"', con=conn, index_col='index')

    for Sym in df.Symbol:
        updatePrice(Sym)
        # print (Sym)
        # print (each_stock)
        # each_stock.to_sql(Sym, conn, if_exists='append')
################################################################################

## 프로그램 파일명 빼고 난 인자값으로 실행
for MARKET in MARKETS:
    get_symbol(MARKET)
    getPrice(MARKET)

"""
<<<<<<< HEAD

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
=======
>>>>>>> eab9008 (init_data.py: 주석 추가, 마켓 입력 버그 수정)
# 모든 종목 테이블 드랍
for Sym in df.Code:
    dropTableStatement = "DROP TABLE \'" + Sym + "\'"
    print(dropTableStatement)
    conn.execute(dropTableStatement)
"""

## DB 연결 종료
conn.close()
