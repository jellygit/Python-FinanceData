#!/bin/env python
import os
import sys
import pandas as pd
import sqlite3

read_conn = sqlite3.connect('./db/finance.db')
write_conn = sqlite3.connect('./db/backtest.db')
fi_cur = read_conn.cursor()
bt_cur = write_conn.cursor()

## 인자 확인: 0는 실행파일 명, 1 이후가 인자
## 비어 있으면 전체 마켓
if len(sys.argv) > 1:
    MARKETS = sys.argv
    MARKETS.pop(0)
else:
    MARKETS = [ "ETF/KR", "KRX" ]
    # MARKETS = [ "ETF/KR", "KRX", "NASDAQ", "NYSE", "SP500" ]

SYMBOLS = pd.DataFrame()

def getSymbols(MARKET):
    df = pd.read_sql('select Symbol from \"' + MARKET + '"', con=read_conn)
    return df

def db_deduplication(Sym):
    QUERY = 'delete from "' + Sym + '" where rowid not in ( select  min(rowid) from "' + Sym +'" group by Date )'
    try:
        fi_cur.execute(QUERY)
        bt_cur.execute(QUERY)
        read_conn.commit()
        write_conn.commit()
    except:
        print("error")
    print(QUERY)


for MARKET in MARKETS:
    append_df = getSymbols(MARKET)
    SYMBOLS = pd.concat([SYMBOLS, append_df])

    for Sym in SYMBOLS.Symbol:
        db_deduplication(Sym)

"""
read_conn = sqlite3.connect('./db/finance.db')
write_conn = sqlite3.connect('./db/backtest.db')
cur = bt_CONN.cursor()

DF = pd.read_sql("select Symbol from \"ETF/KR\"", con=CONN)
#DF = pd.read_sql("select Symbol from KRX", con=CONN)

# DF.Symbol
# 모든 종목코드 테이블 돌면서 날짜 중복된 레코드 삭제
for Sym in DF.Symbol:
    QUERY = 'delete from "' + Sym + '" where rowid not in ( select  min(rowid) from "' + Sym +'" group by Date )'
    try:
        cur.execute(QUERY)
        CONN.commit()
    except:
        print("error")
    print(QUERY)

CONN.close()
"""
read_conn.close()
write_conn.close()

