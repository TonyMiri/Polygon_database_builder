import psycopg2, sys, os, json
from psycopg2 import sql
from datetime import datetime
import polygon_functions_and_keys as poly

PGHOST = 'localhost'
PGDATABASE = 'StonksDB'
PGUSER = 'postgres'
PGPASS = 'postgres'

#=============== CONNECT TO DATABASE =================================================
#Credentials defined in config file
try:
    conn_string = 'host={} port=5432 dbname={} user={} password={}'.format(PGHOST,PGDATABASE,PGUSER,PGPASS)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print('Database Connected!')
except Exception as err:
    cursor = None
    print("psycopg2 error: ", err)

#=============== PREPARING THE DATA ==================================================

def get_data_keys(data):
    keys = []
    for key in data:
        keys.append(key)
    return keys

def get_data_values(data):
    values = []
    for key in data:
        values.append(data[key])
    return values

def get_nested_keys(data): #recursion would make this complete
    unpacked_keys = []
    for key in data:
        if isinstance(data[key], list): 
            for item in data[key]: # for each item in the list
                if isinstance(item, dict):
                    for x in item: #for each key in the dictionary
                        if x not in unpacked_keys:
                            unpacked_keys.append(str(x))
    return unpacked_keys

def get_data_type(value):
    if isinstance(value, str): 
        return 'VARCHAR'
    elif isinstance(value, list):
        return 'text[]'
    elif isinstance(value, bool):
        return 'BOOLEAN'
    elif isinstance(value, int):
            return 'INT' if value < 2147483647 else 'bigint'
    elif isinstance(value, float):
        return 'numeric'
    else:
        return 'VARCHAR'

#=============== WORKING WITH THE DATABASE =========================================

def check_if_table_exists(table):
    cursor.execute(sql.SQL('''SELECT to_regclass('public.{}');''').format(sql.Identifier(table)))
    r = cursor.fetchall()
    if r[0][0] is None:
        return False
    else:
        return True

correct_keys = ['ticker', 'volume','volume_weighted_price', 'open', 'close', 'high', 'low', 'time', 'num_of_transactions']
data_types = ['VARCHAR', 'INT', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric','timestamp', 'INT']

def create_table(table_name, data):
    try:
        if 'results' in data:
            sql_str = '''CREATE TABLE "{}" ('''.format(table_name)
            for x in range(len(correct_keys)):
                sql_str += '{} {}, '.format(correct_keys[x], data_types[x])
            sql_str = sql_str[:-2] + ');'           
            cursor.execute(sql.SQL(sql_str))
            conn.commit()
        else:
            print('{} - No results. Table not created.'.format(table_name))
    except Exception as e:
        print('Error during table creation! STOP!')
        with open('Log_file.txt', 'a') as log:
            log.write(table_name + ' table not created. Exception: ' + str(e))
        sys.exit()

##Will only get company data passed to it
#def populate_company_data(table_name, data):
    #keys = get_data_keys(data)
    #values = get_data_values(data) 
    #sql_str = """INSERT INTO {} ({}) VALUES (""".format(table_name, ', '.join(keys))
    #for key in keys:
        #sql_str += '%s, '
    #sql_str = sql_str[:-2] + ');'
    #try:
        #sql_str = sql_str.replace('similar','"similar"')
    #except:
        #pass
    #cursor.execute(sql_str, tuple(values))
    #conn.commit()
    #pass

#Will only get price data passed to it
def populate_price_data(table_name, data):
    try:
        if check_if_table_exists(table_name):
            if 'results' in data:
                ticker = data['ticker']
                sql_str = '''INSERT INTO "{}" ({}) VALUES '''.format(table_name, ', '.join(correct_keys))
                values = data['results'] #<----- List of dictionaries
                old = ['v', 'vw', 'o', 'c', 'h', 'l', 't', 'n']
                for group in values:
                    temp = {'ticker': ticker, 'volume': 0, 'volume_weighted_price': 0, 'open': 0,
                            'close': 0, 'high': 0, 'low': 0, 'time': None, 'num_of_transactions': 0}
                    date_time = datetime.fromtimestamp(group['t'] / 1000)
                    group['t'] = str(date_time)
                    for x in old:
                        if x in group:
                            if x == 'v':
                                temp.update({'volume': group['v']})
                            elif x == 'vw':
                                temp.update({'volume_weighted_price': group['vw']})
                            elif x == 'o':
                                temp.update({'open': group['o']})
                            elif x == 'c':
                                temp.update({'close': group['c']})
                            elif x == 'h':
                                temp.update({'high': group['h']})
                            elif x == 'l':
                                temp.update({'low': group['l']})
                            elif x == 't':
                                temp.update({'time': group['t']})
                            elif x == 'n':
                                temp.update({'num_of_transactions': group['n']})
                            else:
                                pass
                    #cast the group of values to a tuple then to a string
                    isolated_group = str(tuple(get_data_values(temp)))
                    sql_str += isolated_group.replace(')', '), ')
                sql_str = sql_str[:-2] + ';'
                cursor.execute(sql.SQL(sql_str))
                conn.commit()
            else:
                print('{} No results'.format(table_name))
        else:
            print('{} table doesn\'t exist'.format(table_name))
    except Exception as e:
        print('Error during price insertion! STOP!')
        with open('Log_file.txt', 'a') as log:
            log.write(table_name + ' price data not inserted. Exception: ' + str(e))
        sys.exit()

#===================== SHOWTIME ==========================================================

def build_database():
    #Get all the tickers, create the main table, get data to use when creating tables
    print('Getting tickers...')
    with open('us_tickers_list2.txt', 'r') as f:
        tickers_list = json.load(f)
    print("Got 'em!")
    for ticker in tickers_list:
        create_table('{}_15_min'.format(ticker), poly.get_bars(ticker, '15', 'minute', '2020-04-29', '2021-04-30', '50000'))
        create_table('{}_30_min'.format(ticker), poly.get_bars(ticker, '30', 'minute', '2020-04-28', '2021-04-30', '50000'))
        create_table('{}_1_hour'.format(ticker), poly.get_bars(ticker, '1', 'hour', '2018-04-27', '2021-04-30', '50000'))
        create_table('{}_daily'.format(ticker), poly.get_bars(ticker, '1', 'day', '2016-04-27', '2021-04-30', '50000'))
        populate_price_data('{}_15_min'.format(ticker), poly.get_bars(ticker, '15', 'minute', '2018-04-27', '2019-04-27', '50000'))
        populate_price_data('{}_15_min'.format(ticker), poly.get_bars(ticker, '15', 'minute', '2019-04-28', '2020-04-28', '50000'))
        populate_price_data('{}_15_min'.format(ticker), poly.get_bars(ticker, '15', 'minute', '2020-04-29', '2021-04-30', '50000'))
        populate_price_data('{}_30_min'.format(ticker), poly.get_bars(ticker, '30', 'minute', '2018-04-27', '2020-04-27', '50000'))
        populate_price_data('{}_30_min'.format(ticker), poly.get_bars(ticker, '30', 'minute', '2020-04-28', '2021-04-30', '50000'))
        populate_price_data('{}_1_hour'.format(ticker), poly.get_bars(ticker, '1', 'hour', '2018-04-27', '2021-04-30', '50000'))
        populate_price_data('{}_daily'.format(ticker), poly.get_bars(ticker, '1', 'day', '2016-04-27', '2021-04-30', '50000'))
        print(ticker + ' finished!')
    pass

build_database()

#1095 days is 26,280 hours/52,560 30-min/105,120 15-min/315,360 5-min/1,576,800 minutes