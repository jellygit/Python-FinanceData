#!/bin/env python
import pandas as pd
import sqlite3

CONN = sqlite3.connect('./db/finance.db')
cur = CONN.cursor()

markets = [ "KRX", "NASDAQ", "NYSE", "AMEX", "SP500", "ETF_KR", "ETF_US" ]

def db_sort(CONN, Symbol):
    for Sym in Symbol:
        try:
            QUERY = 'delete from "' + Sym + '" where rowid not in ( select  min(rowid) from "' + Sym +'" group by Date )'
            print(QUERY)
            cur.execute(QUERY)
            CONN.commit()
        except:
            print('error')
    
for market in markets:
    DF = pd.read_sql('select Symbol from \"' + market + '\"', con=CONN)
    try:
        db_sort(CONN, DF.Symbol)
    except:
        print(market + ' Error!')

CONN.close()
