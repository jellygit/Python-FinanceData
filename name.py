#!/bin/env python
import os
import sys
import pandas as pd
import sqlite3

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

CONN = create_connection('./db/finance.db')
BT = create_connection('./db/backtest.db')

file=open('list.txt', 'r')
ALL=file.read()
lines = ALL.split('\n')
for line in lines:
    DF = pd.read_sql('select Symbol, Name from KRX where Symbol like \"' + line +'\" union select Symbol, Name from ETF_KR where Symbol like \"' + line + '\"', con = CONN)
    print (DF)
    # print (line)

CONN.close()
BT.close()
