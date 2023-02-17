#!/usr/bin/env python3
import os
import sys
import FinanceDataReader as fdr
import pandas as pd
import sqlite3
import argparse

## DB 디렉토리 확인 후 없으면 생성
DB_FOLDER = "db/"
if not os.path.isdir(DB_FOLDER):
    os.mkdir(DB_FOLDER)

## sqlite3 DB 연결, 없으면 파일 생성
conn = sqlite3.connect('./db/finance.db')

## 인자 확인: 0는 실행파일 명, 1 이후가 인자
## 비어 있으면 전체 마켓 업데이트
if len(sys.argv) > 1:
    MARKETS = sys.argv
    MARKETS.pop(0)
else:
    MARKETS = [ "ETF/KR" ]
    #MARKETS = [ "ETF/KR", "KRX", "NASDAQ", "NYSE", "SP500" ]

################################################################################
## 함수 모음
## 종목코드 받아오기
## MARKET 에 대해 종목코드를 받아오고 DB 에 저장 -> 테이블 없으면 생성
def update_symbol(MARKET):
    df = fdr.StockListing(MARKET)

    ## 한국만 Symbol 이 아니라 Code 인데 호환성을 위해 Rename
    if MARKET in "KRX":
        df.rename(columns = {'Code' : 'Symbol'}, inplace = True)
    
    if checkTableExists(conn, MARKET):
        insertTable(df, MARKET)
    else:
        createTable(df, MARKET)
        insertTable(df, MARKET)

def insertTable(df, tablename):
    df.to_sql(tablename, conn, if_exists='replace')

def createTable(df, tablename):
    schema = pd.io.sql.get_schema(df, tablename)
    df.to_sql(schema, conn, if_exists="replace")

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

def drop_df_row(exist_df, append_df):
    from_ts = exist_df.index[0]
    to_ts = append_df.index[-1]
    #append_df = append_df[(append_df.index < from_ts | append_df.index > to_ts)]
    return append_df

## 가격 업데이트
## 옵션 -p 추가 시 실행(예정)
def getPrice(MARKET):
    print(MARKET)
    # 거래소별 종목 번호
    df = pd.read_sql('select * from \"' + MARKET + '\"', con=conn, index_col='index')

    for Sym in df.Symbol:
        # 종목 일일 가격 정보 테이블 존재 확인,
        # 없으면 새로 쓰기
        new_stock = fdr.DataReader(Sym)
        new_stock.index = pd.to_datetime(new_stock.index)

        # 기존 가격 존재 시 new_stock 에서 새로운 정보만 남김
        if checkTableExists(conn, Sym):
            db_stock = pd.read_sql('select * from "'+ Sym +'"', con=conn, index_col='Date')
            db_stock.index = pd.to_datetime(db_stock.index)
            from_ts = db_stock.index[0]
            to_ts= db_stock.index[-1]
            new_stock = new_stock[(new_stock.index < from_ts) | (new_stock.index > to_ts)]
        insertTable(new_stock, Sym)

################################################################################

## 프로그램 파일명 빼고 난 인자값으로 실행
for MARKET in MARKETS:
    update_symbol(MARKET)
    getPrice(MARKET)

"""
# 모든 종목 테이블 드랍
for Sym in df.Code:
    dropTableStatement = "DROP TABLE \'" + Sym + "\'"
    print(dropTableStatement)
    conn.execute(dropTableStatement)
"""

## DB 연결 종료
conn.close()

