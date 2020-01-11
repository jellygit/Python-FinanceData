import pandas as pd
from pandas import Series, DataFrame
import sqlite3

CONN = sqlite3.connect('./db/finance.db')
cur = CONN.cursor()

DF = pd.read_sql("select Symbol from KRX", con=CONN)

DF.Symbol
for Sym in DF.Symbol:
    # QUERY = 'delete from "' + Sym + '" where rowid not in ( select  min(rowid) from "' + Sym +'" group by Date )'
    QUERY = 'delete from "' + Sym + '" where rowid not in ( select  min(rowid) from "' + Sym +'" group by Date )'
    cur.execute(QUERY)
    CONN.commit()
    print(QUERY)

CONN.close()
