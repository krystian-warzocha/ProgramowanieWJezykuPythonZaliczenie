# -*- coding: utf-8 -*-

import sqlite3

db_path = 'stockindex.db'
conn = sqlite3.connect(db_path)

c = conn.cursor()

#
# Tabele
#
try:
    c.execute('''
              DROP TABLE StockIndexQuote
              ''')
except Exception as e:
    print e

try:
    c.execute('''
              DROP TABLE StockIndex
              ''')
except Exception as e:
    print e

c.execute('''
          CREATE TABLE StockIndex
          ( id INTEGER PRIMARY KEY,
            symbol VARCHAR(100) NOT NULL,
            mean FLOAT,
            variance FLOAT,
            skew FLOAT,
            kurtosis FLOAT
          )
          ''')

c.execute('''
          CREATE TABLE StockIndexQuote
          ( value NUMERIC NOT NULL,
            value_date DATE NOT NULL,
            stockindex_id INTEGER,
            FOREIGN KEY(stockindex_id) REFERENCES StockIndex(id),
	    PRIMARY KEY (value_date, stockindex_id))
          ''')
