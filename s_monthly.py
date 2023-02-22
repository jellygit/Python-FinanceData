#!/usr/bin/env python3
import os
import sys
import FinanceDataReader as fdr
import pandas as pd
import sqlite3
import argparse
import datetime as dt

## 시간 설정
now = dt.datetime.now()
TODAY = now.strftime('%Y-%m-%d')

## DB 디렉토리 확인 후 없으면 생성
DB_FOLDER = "db/"
if not os.path.isdir(DB_FOLDER):
    os.mkdir(DB_FOLDER)

## CSV 디렉토리 확인 후 없으면 생성
CSV_FOLDER = "csv/"
if not os.path.isdir(CSV_FOLDER):
    os.mkdir(CSV_FOLDER)

## sqlite3 DB 연결, 없으면 파일 생성
read_conn = sqlite3.connect('./db/finance.db')
write_conn = sqlite3.connect('./db/backtest.db')

## 인자 확인: 0는 실행파일 명, 1 이후가 인자
## 비어 있으면 전체 마켓 업데이트
if len(sys.argv) > 1:
    MARKETS = sys.argv
    MARKETS.pop(0)
else:
    MARKETS = [ "ETF/KR", "KRX" ]
    # MARKETS = [ "ETF/KR", "KRX", "NASDAQ", "NYSE", "SP500" ]

################################################################################
## 함수 모음
## get_symbol_frm_db
## 역할: 종목코드 받아오기
## 인자: MARKET, 거래소 심볼
## 동작: MARKET 을 받아 해당 MARKET 대해 DB 에서 종목코드를 읽어 DataFrame 을 리턴
## MARKET 테이블이 없는 경우 그냥 끝
def get_symbol_frm_db(MARKET):
    if checkTableExists(read_conn, MARKET):
        df = pd.read_sql('select Symbol from \"' + MARKET + '\"', con=read_conn)
        return df

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
def getMonthly(SYMBOL):
    df = pd.read_sql('select * from \"' + SYMBOL + '\"', con=conn)



## 테이블 존재 유무 확인
## sqlite3 마스터 테이블에 tablename 으로 질의: 존재하면 True / 없으면 False Return
def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM sqlite_master 
        WHERE name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        return True
    print(tablename + " not exsit")
    return False

def getMonthlyPrice(Symbol):
    # DB 에서 개별 종목 가격 이력을 받아옴
    df = pd.read_sql('select * from \"' + Symbol + '\"', con=read_conn, index_col='Date')
    # START_DATE = df.index[0]
    # END_DATE   = df.index[-1]

    # index(Date)에서 연도만 가져온 뒤 중복 제거
    df_tmp = pd.DatetimeIndex(df.index).year
    years = df_tmp.unique()

    # 중복 제거된 연도를 대상으로 가격 가져오기
    each_stock = pd.DataFrame()
    for year in years:
        for month in range(1, 13):
            for day in range(1,15):
                date = dt.datetime(year, month, day, 0, 0, 0)
                date = date.__format__("%Y-%m-%d %H:%M:%S")

                if date in df.index:
                    append_df = pd.read_sql('select * from \"' + Symbol + '\" where Date = \"' + date + '\"', con=read_conn, index_col='Date')

                    each_stock = pd.concat([each_stock, append_df])
                    break
    if each_stock.empty:
        print("empty")
    else:
        each_stock.to_sql(Symbol, write_conn, if_exists='replace')


################################################################################

## 프로그램 파일명 빼고 난 인자값으로 실행
for MARKET in MARKETS:
    # get_symbol_frm_db 에서 dataframe 을 받음, 테이블이 없는 경우 못 받아오고,
    # df_Sym 에 아무 값이 없으므로 다음 MARKET 실행
    df_Sym = get_symbol_frm_db(MARKET)

    # MARKET 심볼 목록을 하나씩 table 유무 확인
    # 존재 시 매월 초 가격 리턴
    # 없으면 다음 종목
    if df_Sym is not None:
        for Sym in df_Sym.Symbol:
            # Sym 이 존재하는 테이블인지 체크, 존재하면 가격 업데이트
            if checkTableExists(read_conn, Sym):
                print(Sym)
                getMonthlyPrice(Sym)


## DB 연결 종료
read_conn.close()
write_conn.close()
