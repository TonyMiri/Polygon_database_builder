import psycopg2, sys, os, json
from psycopg2 import sql

host = 'localhost'
database = 'StonksDB'
user = 'postgres'
password = 'postgres'
db_conn_string = "host={} port = 5432 dbname={} user={} password={}".format(host,database,user,password)

try:
    conn = psycopg2.connect(db_conn_string)
    cur = conn.cursor()
    print('Database Connected!')
except Exception as err:
    cur = None
    print('Database connection failed! ', err)


#sql_str1 = """SELECT tablename
 #   FROM pg_catalog.pg_tables
  #  WHERE schemaname != 'pg_catalog' AND 
   #     schemaname != 'information_schema';"""
#cur.execute(sql_str1)
#tables = cur.fetchall()

def get_em(data):
    tickers_list = []
    for table in data:
        cur.execute(sql.SQL("""SELECT ticker FROM {} LIMIT 1;""").format(sql.Identifier(table[0])))
        ticker = cur.fetchall()
        if ticker[0][0] not in tickers_list:
            tickers_list.append(ticker[0][0])
            print(ticker[0][0])
    return tickers_list


#with open('db_tickers.txt', 'w') as db_tickers:
 #   db_tickers.write(str(get_em(tables)))

def populate_main_table():
    tickers_list = open('db_tickers.txt', 'r').read().replace('[', '(').replace(']',')')
    sql_string = ("""INSERT INTO stonks (ticker) VALUES {};""".format(tickers_list)
                    .replace("'", "('")
                    .replace("(',", "'),")
                    .replace('((', '(')
                    .replace('),)', ');')
    )
    print(sql_string)
    #cur.execute(sql_string)
    #conn.commit()
    pass

#populate_main_table()