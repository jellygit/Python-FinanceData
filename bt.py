#!/usr/bin/env python3
import os
import sys
import pandas as pd
import sqlite3
import datetime as dt
import asyncio

pd.set_option('display.max_rows', None)

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
    MARKETS = [ "KRX", "ETF/KR" ]
    # MARKETS = [ "ETF/KR", "KRX", "NASDAQ", "NYSE", "SP500" ]

# 매월 투입금
BUDGET = 500000
USA = [ "NASDAQ", "NYSE", "SP500", "ETF/US", "AMEX" ]

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

# 수수료 계산 함수.
def F_FEE(Get_Date):
    FEE_STD = dt.datetime(2019, 6, 1, 0, 0, 0)
    FEE_STD = FEE_STD.__format__("%Y-%m-%d %H:%M:%S")
    if Get_Date > FEE_STD:
        return 0.0013
    else:
        return 0.0018

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

async def create_bt(Symbol, block=False):
    # DB 에서 개별 종목 가격 이력을 받아옴
    # df = pd.read_sql('select * from \"' + Symbol + '\"', con=write_conn, index_col='Date')

    if checkTableExists(write_conn, Symbol):
        df = pd.read_sql('select * from ( select * from "' + Symbol + '" order by Date DESC limit 120 ) order by Date ASC', con=write_conn, index_col='Date')

        # 거스름돈
        WALLET = 0
        # 보유주식 수
        STOCK = 0
        # 평가액
        PRINCIPAL = 0
    
        result_df = pd.DataFrame()
    
        TEMP_DF = pd.DataFrame()
        RES_DF = pd.DataFrame()
        for date in df.index:
            # 주식 매수
            STOCK = (BUDGET + WALLET) // ((df.at[date, 'Close'])) + STOCK
            WALLET = (BUDGET + WALLET) % ((df.at[date, 'Close']))
            PRINCIPAL += BUDGET
            VALUATION = df.at[date, 'Close'] * STOCK + WALLET
            FEE = VALUATION * F_FEE(date)
            VALUATION = round((VALUATION - FEE), 2)
            #VALUATION = int((VALUATION - FEE))
            RATE = round(((VALUATION / PRINCIPAL) - 1), 2)
            date = date.replace(' 00:00:00', '')
    
            TEMP_DF = pd.DataFrame({
                'PRINCIPAL':PRINCIPAL,
                'RATE':(RATE*100),
                'STOCK':STOCK,
                'VALUATION':"%.2f"%VALUATION}, index = [date] )
            RES_DF = pd.concat([RES_DF, TEMP_DF])
        RES_DF.index.name = 'Date'
        print(RES_DF)
        
        bt_Sym = "bt_" + Symbol
        RES_DF.to_sql(bt_Sym, write_conn, if_exists='replace')
        
        CSV_FN = CSV_FOLDER + '%s.csv' % (Symbol)
        # CSV  파일 출력방향 지정
        sys.stdout = open(CSV_FN, 'w')
        # CSV 파일로 저장
        print(RES_DF)
        # 출력방향 원복
        sys.stdout = sys.__stdout__

    else:
        print("not exist")

async def main(MARKET, df_Sym):
    # MARKET 심볼 목록을 하나씩 table 유무 확인
    # 존재 시 매월 초 가격 리턴
    # 없으면 다음 종목
    if df_Sym is not None:
        tasks = [asyncio.create_task(create_bt(Sym))
                 for Sym in df_Sym.Symbol]
        await asyncio.gather(*tasks)


################################################################################

## 프로그램 파일명 빼고 난 인자값으로 실행
for MARKET in MARKETS:
    # get_symbol_frm_db 에서 dataframe 을 받음, 테이블이 없는 경우 못 받아오고,
    # df_Sym 에 아무 값이 없으므로 다음 MARKET 실행
    print(MARKET)
    df_Sym = get_symbol_frm_db(MARKET)
    print(df_Sym)

    if MARKET in USA:
        BUDGET = 500
    else:
        BUDGET = 500000

    asyncio.run(main(MARKET, df_Sym))



## DB 연결 종료
read_conn.close()
write_conn.close()
