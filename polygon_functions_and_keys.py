import requests, json, math

#=============== CONNECT TO POLYGON ==================================================
#Credentials defined in config file
POLYGON_API_KEY = 'YOUR_API_KEY'
POLYGON_BASE_URL = 'https://api.polygon.io'

#Get tickers from Polygon
#accepts parameters: sort, type, market, locale, search, perpage, page, active
#example: https://api.polygon.io/v2/reference/tickers?sort=ticker&type=cs&market=STOCKS&perpage=50&page=1&apiKey=YOUR_API_KEY
POLYGON_TICKERS = 'https://api.polygon.io/v2/reference/tickers?'
SINGLE_TICKER = 'https://api.polygon.io/v1/meta/symbols/{}/company?{}'
BARS_URL = 'https://api.polygon.io/v2/aggs/ticker/{}/range/{}/{}/{}/{}?' 


#================ GET DATA ============================================================

#------- Functions to get all US tickers and all world tickers --------------

def get_us_tickers_list():
    #Make API call to get number of pages
    r = requests.get('{}{}{}'.format(POLYGON_TICKERS, 'locale=us', POLYGON_API_KEY)).json()
    page_count = math.ceil(int(r['count'])/int(r['perPage']))
    #Loop through pages and pull every ticker from the response data
    tickers = []
    for page in range(1, page_count+1):
        if page % 10 == 0:
            print('Page ' + str(page) + ' of ' + str(page_count))
        api_url = '{}{}{}{}'.format(POLYGON_TICKERS, 'locale=us&page=', page, POLYGON_API_KEY)
        response = requests.get(api_url).json()
        for stock in response['tickers']:
            tickers.append(stock['ticker'])
    return tickers

#Probably just make these one function and add the optional arguments

def get_all_tickers_list():
    #Make API call to get number of pages
    r = requests.get('{}{}'.format(POLYGON_TICKERS, POLYGON_API_KEY)).json()
    page_count = math.ceil(int(r['count'])/int(r['perPage']))
    #Loop through pages and pull every ticker from the response data
    tickers = []
    for page in range(1, page_count+1):
        api_url = '{}{}{}{}'.format(POLYGON_TICKERS, 'page=', page, POLYGON_API_KEY)
        response = requests.get(api_url).json()
        for stock in response['tickers']:
            tickers.append(stock['ticker'])
    return tickers

def get_multi_sample():
    return(requests.get('{}{}'.format(POLYGON_TICKERS, POLYGON_API_KEY)).json())

#------ Functions to get data for specific ticker. MUST USE CAPITAL LETTERS FOR TICKER -------

def get_company_data(ticker):
    return(requests.get(SINGLE_TICKER.format(ticker, POLYGON_API_KEY)).json())

#------ Getting price data -------------------------------------------------------------------

#Date format is yyyy-mm-dd, adjust is boolean(false by default), sort is 'asc' or 'desc'
#limit is number of base aggregates(minute or daily) queried to create the aggregate results
def get_bars(ticker, multiplier, interval, start_date, end_date, limit, unadjusted=None, sort=None):
    url_string = BARS_URL.format(ticker, multiplier, interval, start_date, end_date)
    if (unadjusted is None) and (sort is None):
        url_string += 'limit={}{}'.format(limit, POLYGON_API_KEY)
    elif (unadjusted) and (sort is None) :
        url_string += 'unadjusted=true&{}{}'.format(limit, POLYGON_API_KEY)
    elif (unadjusted is None) and (sort):
        url_string += 'sort={}&{}{}'.format(sort, limit, POLYGON_API_KEY)
    else:
        url_string += 'unadjusted=true&sort={}&limit={}{}'.format(sort, limit, POLYGON_API_KEY)
    return(requests.get(url_string).json())

#============== POLYGON SPECIFIC DATABASE FUNCTIONS =============================

#with open('us_tickers_list.txt', 'x') as output:
 #   json.dump(get_us_tickers_list(), output, indent=2)