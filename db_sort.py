#!/bin/env python
import pandas as pd
import sqlite3

CONN = sqlite3.connect('./db/finance.db')
cur = CONN.cursor()

DF = pd.read_sql("select Symbol from KRX", con=CONN)

# DF.Symbol
# 모든 종목코드 테이블 돌면서 날짜 중복된 레코드 삭제
for Sym in DF.Symbol:
    QUERY = 'delete from "' + Sym + '" where rowid not in ( select  min(rowid) from "' + Sym +'" group by Date )'
    cur.execute(QUERY)
    CONN.commit()
    print(QUERY)

CONN.close()
