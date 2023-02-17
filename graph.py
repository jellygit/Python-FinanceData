#!/usr/bin/env python3
import os
import sys
import pandas as pd
import sqlite3
import datetime as dt
import matplotlib.ticker as mtick
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as dates

# 폰트 설정
mpl.rc('font', family='NanumGothic')
mpl.rc('axes', unicode_minus=False)

## DB 디렉토리 확인 후 없으면 생성
DB_FOLDER = "db/"
if not os.path.isdir(DB_FOLDER):
    os.mkdir(DB_FOLDER)

## sqlite3 DB 연결, 없으면 파일 생성
read_conn = sqlite3.connect('./db/finance.db')
write_conn = sqlite3.connect('./db/backtest.db')

PNG_FOLDER = "png/"
if not os.path.isdir(PNG_FOLDER):
    os.mkdir(PNG_FOLDER)

## 인자 확인: 0는 실행파일 명, 1 이후가 인자
## 비어 있으면 전체 마켓 업데이트
if len(sys.argv) > 1:
    MARKETS = sys.argv
    MARKETS.pop(0)
else:
    MARKETS = [ "ETF/KR" ]
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

def create_graph(MARKET, Symbol):
    # DB 에서 개별 종목 가격 이력을 받아옴
    bt_Sym = "bt_" + Symbol
    if checkTableExists(write_conn, bt_Sym):
        RESULT_DF = pd.read_sql('select * from ( select * from "' + bt_Sym + '" order by Date DESC limit 120 ) order by Date ASC', con=write_conn, index_col='Date')
    
    
        #TITLE = pd.read_sql('select Symbol, Name from KRX where Symbol like \"' + Symbol + '\" union select Symbol, Name from ETF_KR where Symbol like \"' + Symbol + '\"', con = CONN, index_col="Symbol")
        TITLE = pd.read_sql('select Symbol, Name from "' + MARKET + '" where Symbol like \"' + Symbol + '\"', con = read_conn, index_col="Symbol")
        """
        PLOT = RESULT_DF.plot(secondary_y=['RATE'], mark_right=False)
        # ax.yaxis.set_major_formatter(mtick.PercentFormatter())
        PLOT.set_title(Symbol + ' ' + TITLE.at[Symbol, 'Name'])
        PLOT.right_ax.yaxis.set_major_formatter(mtick.PercentFormatter())
        PLOT.ticklabel_format(axis='y', style='plain')
        """
        PLOT = RESULT_DF.plot(xlabel='Date', ylabel='KRW', title=(Symbol + ' ' + TITLE.at[Symbol, 'Name']), logy=True, rot=45)

        # 그래프 파일 저장
        CSV_FIG = PNG_FOLDER + '%s.png' % (Symbol)
        FIG = PLOT.get_figure()
        FIG.savefig(CSV_FIG)
        plt.close()
    else:
        print("not exist")

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
                create_graph(MARKET, Sym)


## DB 연결 종료
read_conn.close()
write_conn.close()
