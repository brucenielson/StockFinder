# ****Data Layer****
# The Data Layer is responsible for obtaining data from either the web or the database
# It will also contain the database utilities necessary to create the database.

import urllib2
import json
import datetime
import time
import lxml.html
import sqlite3 as sqlite
import os

__author__ = 'Bruce Nielson'

KEY_STATS_FIELDS = "symbol, TotalDebt, ReturnOnEquity, TrailingPE, RevenuePerShare, MarketCap, " \
         + "PriceBook, EBITDA, PayoutRatio, OperatingCashFlow, Beta, ReturnonAssests, "\
         + "TrailingAnnualDividendYield, ForwardAnnualDividendRate, p_5YearAverageDividentYield, "\
         + "DividendDate, Ex_DividendDate, ForwardAnnualDividendYield, SharesShort, "\
         + "CurrentRatio, BookValuePerShare, ProfitMargin, TotalCashPerShare, QtrlyEarningsGrowth, "\
         + "TotalCash, Revenue, ForwardPE, DilutedEPS, OperatingMargin, SharesOutstanding, "\
         + "TotalDebtEquity"


QUOTES_FIELDS = "Symbol, LastTradePriceOnly, YearLow, YearHigh, DividendShare, " \
         + "EarningsShare, PERatio, PriceSales, PEGRatio, ShortRatio, " \
         + "BookValue, PriceBook"

STOCKS_FIELDS = "symbol, Industry, Sector, start, FullTimeEmployees"

DIVIDEND_HISTORY_FIELDS = "Symbol, Dividends, Date"


ALL_FIELDS = QUOTES_FIELDS + ", " + KEY_STATS_FIELDS + ", " + STOCKS_FIELDS

# Order of fields in Stock List table
ROWID = 0
SYMBOL = 1
INDUSTRY = 2
SECTOR = 3
START = 4
FULL_TIME_EMPLOYEES = 5
HAS_DIVIDENDS = 6
LAST_DIVIDEND_DATE = 7
LAST_UPDATED = 8

## TODO:
## Try this. Might be faster as one query.
## select * from yql.query.multi where queries="SELECT * FROM yahoo.finance.quotes WHERE symbol='T'; SELECT * FROM yahoo.finance.keystats WHERE symbol='T'"





# Create the SQLite database to store stock information for all stocks we're
# interested in. Stock data that doesn't need to get updated all the time is
# stored here. There is an optional parameter to specify a database name.
# This is only used for unit testing so that I don't have to destroy the
# existing database each time the test runs.
def create_database(database_name = "stocksdata.db"):
    # Create or open the database
    db = sqlite.connect(os.path.dirname(__file__)+"\\"+database_name)

    # if database already exists, drop all tables first
    db.execute('drop index if exists symbolx')
    db.execute('drop index if exists labelx')
    db.execute('drop table if exists stock_list')
    db.execute('drop table if exists dividend_history')
    db.execute('drop table if exists label')
    db.execute('drop table if exists url')


    # create the tables
    db.execute('create table stock_list(symbol, industry,'\
                        + 'sector, start, full_time_employees, has_dividends, '\
                        + 'last_dividend_date, last_updated)')
    db.execute('create table dividend_history (stockid, dividends, date)')
    db.execute('create table label(stockid, label)')
    db.execute('create table url(stockid, url)')
    db.execute('create index symbolx on stock_list(symbol)')
    db.execute('create index labelx on label(label)')
    db.commit()
    db.close()



# Get a list of all current S&P 500 stocks off of Wikipedia.
# Since Wikipedia is constantly updated, this should always be an
# up to date list.
def get_wikipedia_snp500_list():
    #Download and parse the Wikipedia list of S&P500
    #constituents using requests and libxml.
    #Returns a list symbols from off wikipedia

    # Stores the current time, for the created_at record
    now = datetime.datetime.utcnow()

    # Use libxml to download the list of S&P500 companies and obtain the symbol table
    page = lxml.html.parse('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    symbol_list = page.xpath('//table[1]/tr/td[1]/a/text()')

    return symbol_list




# Takes a list of symbols in format ['AAPL', 'MSFT'] and stores them in
# the database so that the program will track them in the future.
# This function also "primes" the data by looking up rarely changing data
# and storing it so that we don't have to look it up each time we refresh
# data off the web. This includes "stocks" data as well as dividend history.
# If you pass a symbol that already exists in the database it just ignores it.
# There is an optional parameter to specify a database name. This is only used
# for unit testing so that I don't have to destroy the existing database each
# time the test is ran.
def track_stock_symbols(symbol_list, database = "stocksdata.db"):
    if type(symbol_list) != type(list()):
        raise Exception("symbol_list must be a list")

    # get the list of stocks currently in the database to compare to the passed list
    db = sqlite.connect(os.path.dirname(__file__)+"\\"+database)
    cursor = db.cursor()
    cursor.execute("select symbol, rowid from stock_list")
    db_list = cursor.fetchall()
    # convert db_list to just be a list of symbols (not in tuples)
    db_list_symbols = [tuple[0] for tuple in db_list]
    new_symbols = [symbol for symbol in symbol_list if symbol not in db_list_symbols]

    # If there are no new symbols in the list, then just ignore everything and abort
    if len(new_symbols) == 0:
        return

    # Get data to store for this new symbol
    stocks_data = get_stocks_data(new_symbols)
    div_hist_data = get_dividend_history_data(new_symbols)

    insert_stocks_list = []
    insert_div_hist_list = []

    for symbol in new_symbols:
        # get stocks data
        industry = stocks_data[symbol]["Industry"]
        sector = stocks_data[symbol]["Sector"]
        start = stocks_data[symbol]["start"]
        full_time_employees = stocks_data[symbol]["FullTimeEmployees"]
        if len(div_hist_data) > 0:
            has_dividends = True
        else:
            has_dividends = False
        try:
            last_dividend_date = div_hist_data[symbol]["DividendHistory"][0]['Date']
        except:
            last_dividend_date = None
        last_updated = datetime.datetime.now()

        stocks_data_row = (symbol, industry, sector, start, full_time_employees, has_dividends, last_dividend_date, last_updated)
        insert_stocks_list.append(stocks_data_row)



    # Do insert for new symbols that we want to store
    db.executemany("insert into stock_list(symbol, industry, sector, start, full_time_employees, has_dividends, last_dividend_date, last_updated) "\
            + "values (?, ?, ?, ?, ?, ?, ?, ?)", insert_stocks_list)
    db.commit()



    # get rowids for list of new_symbols (that was just inserted into the table)
    stock_list = retrieve_stock_list_from_db()
    stock_ids = {}

    for row in stock_list:
        if row[1] in new_symbols:
            stock_ids[row[1]] = row[0]


    # get dividend history data for new stocks. Ignore ones already in database.
    for symbol in div_hist_data:
        if symbol in new_symbols:
            # look up stock id for this symbol
            id = stock_ids[symbol]

            if id == None or id == "":
                raise Exception("Attempting to insert dividend history for " + symbol + ". But it is not in the database.")

            for div_row in div_hist_data[symbol]['DividendHistory']:
                div_hist_row = (id, div_row['Dividends'], div_row['Date'])
                insert_div_hist_list.append(div_hist_row)


    # Do insert for div history we want to store
    db.executemany("insert into dividend_history(stockid, dividends, date) "\
            + "values (?, ?, ?)", insert_div_hist_list)
    db.commit()

    db.close()



def get_stock_and_dividend_history_data(symbol_list):
    """
    get_stock_and_dividend_history_data() takes a symbol list and returns Stocks and Dividend History data for storing
    in the database.

    :param symbol_list: a list of stock ticker symbols in upper case, i.e. ['AAPL', 'T', 'MSFT'] etc.
    :return: returns a data object that is a dictionary of stock symbols with the data as sub entries,
    i.e. {'AAPL': {Industry: 'Electronics', 'DividendHistory': {... dividend history}. It will contain
    "Stocks" data and Dividend History, both of which are to be stored in the database rather than
    retrieved in real time.
    """

    if type(symbol_list) != type(list()):
        raise Exception("symbol_list must be a list")

    # make a copy of the list so that we can re-run yql until we get the full list
    #remaining_symbols = symbol_list[:]
    remaining_symbols = [symbol.upper() for symbol in symbol_list]
    result = {}
    final = {}

    # Since YQL fails a lot, do a loop until you get everything in the whole
    # original list of symbols
    while len(remaining_symbols) > 0:
        yql = "select * from yql.query.multi where queries = \""\
            + "SELECT * FROM yahoo.finance.stocks WHERE symbol in ("\
            + '\'' \
            + '\',\''.join(remaining_symbols) \
            + '\'' \
            + "); " \
            + "SELECT * FROM yahoo.finance.dividendhistory WHERE "\
             + "startDate = \'\' and endDate = \'\' and symbol in (" \
            + '\'' \
            + '\',\''.join(remaining_symbols) \
            + '\'' \
            + ")\""

        #yql = yql % (today.year-10, today.month, today.day, today.year, today.month, today.day)
        result = execute_yql(yql)
        remaining_symbols = [symbol for symbol in remaining_symbols if symbol not in result.keys()]
        final = dict(final.items() + result.items())

    return final #standardize_data(final)





# Given a list of symbols, get the "stocks" data from Yahoo to
# store in the database. Returns in a standardized format.
def get_stocks_data(symbol_list):

    if type(symbol_list) != type(list()):
        raise Exception("symbol_list must be a list")

    # make a copy of the list so that we can re-run yql until we get the full list
    remaining_symbols = symbol_list[:]
    remaining_symbols = [symbol.upper() for symbol in remaining_symbols]
    result = {}
    final = {}

    # Since YQL fails a lot, do a loop until you get everything in the whole
    # original list of symbols
    while len(remaining_symbols) > 0:
        yql = "select "+ STOCKS_FIELDS +" from yahoo.finance.stocks where symbol in (" \
                        + '\'' \
                        + '\',\''.join(remaining_symbols) \
                        + '\'' \
                        + ")"

        result = execute_yql(yql)
        remaining_symbols = [symbol for symbol in remaining_symbols if symbol not in result.keys()]
        final = dict(final.items() + result.items())

    return standardize_data(final)



# From a list of symbols, get dividend history data. Returns in a
# standardized format.
def get_dividend_history_data(symbol_list):

    if type(symbol_list) != type(list()):
        raise Exception("symbol_list must be a list")

    today = datetime.datetime.now()

    # make a copy of the list so that we can re-run yql until we get the full list
    remaining_symbols = symbol_list[:]
    remaining_symbols = [symbol.upper() for symbol in remaining_symbols]
    result = {}
    final = {}

    # Since YQL fails a lot, do a loop until you get everything in the whole
    # original list of symbols
    while len(remaining_symbols) > 0 and result != None:
        yql = "select "+ DIVIDEND_HISTORY_FIELDS +" from yahoo.finance.dividendhistory where"\
                        + " startDate = \"\" and endDate = \"\""\
                        + " and symbol in (" \
                        +'\'' \
                        + '\',\''.join(remaining_symbols) \
                        + '\'' \
                        + ")"

        # yql = yql % (today.year-10, today.month, today.day, today.year, today.month, today.day)
        result = execute_yql(yql)

        if result != None:
            remaining_symbols = [symbol for symbol in remaining_symbols if symbol not in result.keys()]
            final = dict(final.items() + result.items())

    return standardize_dividend_history_data(final)




# Fix output so that there isn't such inconsistency in the data.
# i.e. "N/A" = 0.00 for a dividend, etc. This function can take any data
# set (quotes, stocks, key_stats) except Dividend History.
def standardize_data(data):
    all_fields = ALL_FIELDS.split(", ")
    for row in data:
        for item in all_fields:

            # If item is not in this quote, then fill in with default data
            if item not in data[row]:
                data[row][item] = None


            #if item is dollar, integer or decimal
            if item in ['LastTradePriceOnly', 'YearLow', 'YearHigh', 'DividendShare', 'EarningsShare',\
                        'PERatio', 'PriceSales', 'PEGRatio', 'ShortRatio', 'BookValue', 'PriceBookTotalDebt', \
                        'ReturnOnEquity', 'TrailingPE', 'RevenuePerShare', 'MarketCap','PriceBook', 'EBITDA', \
                        'OperatingCashFlow', 'Beta', 'ReturnonAssests', 'ForwardAnnualDividendRate', \
                        'SharesShort', 'CurrentRatio', 'BookValuePerShare', 'TotalCashPerShare', 'TotalCash', \
                        'Revenue', 'ForwardPE', 'DilutedEPS', 'SharesOutstanding', 'TotalDebtEquity', \
                        'FullTimeEmployees', 'TotalDebt']:

                value = data[row][item]
                value = str(value).replace(",","")

                if value == "N/A" or value == None or value == "None":
                    data[row][item] = 0.00
                elif type(value) == type(float):
                    data[row][item] = float(value)
                elif is_number(value):
                    data[row][item] = float(value)
                elif value[len(value)-1] == "M":
                    data[row][item] = float(value[:len(value)-1])*1000000.00
                elif value[len(value)-1] == "B":
                    data[row][item] = float(value[:len(value)-1])*1000000000.00
                elif value[len(value)-1] == "T":
                    data[row][item] = float(value[:len(value)-1])*1000000000000.00
                else:
                    raise Exception("For "+row+", "+item+": "+value+" is not a valid value.")


            # if item is a date
            if item in ['Ex_DividendDate', 'start', 'DividendDate']:
                value = data[row][item]

                if value == "N/A" or value == None or value == "None" or "NaN" in value:
                    data[row][item] = None
                else:
                    try:
                        data[row][item] = datetime.datetime.strptime(value,"%Y-%m-%d")
                    except ValueError:
                        try:
                            data[row][item] = datetime.datetime.strptime(value,"%b %d, %Y")
                        except ValueError:
                            raise ValueError("For "+row+", "+item+": "+value+" Incorrect data format for a date. Should be YYYY-MM-DD.")

            # if item is a percentage
            if item in ['QtrlyEarningsGrowth', 'PayoutRatio', 'ProfitMargin', 'TrailingAnnualDividendYield',\
                        'ForwardAnnualDividendYield', 'p_5YearAverageDividentYield', 'OperatingMargin']:

                value = data[row][item]

                if value == "N/A" or value == None or value == "None":
                    data[row][item] = 0.0
                else:
                    try:
                        data[row][item] = float(value.strip('%').replace(",","")) / 100.0
                    except ValueError:
                        raise ValueError("For "+row+", "+item+": "+value+" is not a valid value.")

    return data




# Fix output so that there isn't such inconsistency in the data.
# i.e. "N/A" = 0.00 for a dividend, etc. This function takes only dividend history data.
def standardize_dividend_history_data(div_history_data):
    div_hist_fields = DIVIDEND_HISTORY_FIELDS.split(", ")

    for symbol in div_history_data:
        dividend_list =  div_history_data[symbol]['DividendHistory']
        for div in dividend_list:
            for field in div_hist_fields:
                # If field is not in this quote, then fill in with default data
                if field not in div:
                    div[field] = None

                #if item is dollar, integer or decimal
                if field in ['Dividends']:
                    try:
                        div[field] = convert_to_float(div[field])
                    except ValueError:
                        raise ValueError("For "+symbol+", "+field+": "+str(div[field])+" is not a valid value.")

                # if item is a date
                if field in ['Date']:
                    try:
                        div[field] = convert_to_date(div[field])
                    except ValueError:
                        raise ValueError("For "+symbol+", "+field+": "+str(div[field])+" Incorrect data format for a date. Should be YYYY-MM-DD.")

    return div_history_data



# Attempt to convert value to a float (decimal) format or else raise a
# ValueError exception
def convert_to_float(value):
    value = str(value).replace(",","")

    if value == "N/A" or value == None or value == "None":
        return 0.00
    elif type(value) == type(float):
        return float(value)
    elif is_number(value):
        return float(value)
    elif value[len(value)-1] == "M":
        return float(value[:len(value)-1])*1000000.00
    elif value[len(value)-1] == "B":
        return float(value[:len(value)-1])*1000000000.00
    elif value[len(value)-1] == "T":
        return float(value[:len(value)-1])*1000000000000.00
    else:
        raise ValueError()





# Attempt to convert value to a date format or else raise a
# ValueError exception
def convert_to_date(value):
    if value == "N/A" or value == None or value == "None" or "NaN" in value:
        return None
    else:
        try:
            return datetime.datetime.strptime(value,"%Y-%m-%d")
        except ValueError:
            try:
                return datetime.datetime.strptime(value,"%b %d, %Y")
            except ValueError:
                raise ValueError()




# Pass a string and return True if it can be converted to a float without
# an exception
def is_number(s):
    s = str(s)
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False




# Takes a Yahoo Query Language statement and executes it via the Yahoo API
# Standardizes the format so that the return result is a dictionary with the
# symbol (in upper case) is the key for the data.
def execute_yql(yql):
    url = "http://query.yahooapis.com/v1/public/yql?q=" \
            + urllib2.quote(yql) \
            + "&format=json&env=http%3A%2F%2Fdatatables.org%2Falltables.env&callback="

    try:
        result = urllib2.urlopen(url)
    except urllib2.HTTPError, e:
        raise Exception("HTTP error: ", e.code, e.reason)
    except urllib2.URLError, e:
        raise Exception("Network error: ", e.reason)


    result = json.loads(result.read())

    json_data = result['query']['results']

    # If multiple rows come back, it comes back as a list inside "quote" or whatever the type is
    # {u'quote': [{u'YearLow': u'70.5071',...

    # If just one row comes back, it isn't in a list:
    # {u'quote': {u'YearLow': u'70.5071', ...

    # Dividend history is a bit different, as it should have a list.
    # {u'quote': [{u'Date': u'2014-02-06', u'Dividends': u'0.435710', u'Symbol': u'AAPL'}, {u...
    # Oops,new wrinkle, Dividend history, if it only has one dividend, does not come back as a list:
    #{u'quote': {u'Date': u'2014-11-06', u'Dividends': u'0.470000', u'Symbol': u'AAPL'}}

    # But doing a multi return is different:
    # "results" at front, then a list of "stock" data. First one:
    # {u'results': [{u'stock': [{u'Sector': u'Consumer Goods',...
    # Next one:
    # {u'Sector': u'Technology', u'end': u'2015-01-01',...
    # Finally dividend history all mixed together:
    # {u'quote': [{u'Date': u'2014-02-06', u'Dividends': u'0.435710', u'Symbol': u'AAPL'}, ...


    data_dict = {}

    # If this has an extra "results" then its a multi select. Remove "result"
    # and then grab the different types of data (i.e. "stock" and "dividend") portions
    # into separate pieces
    if 'results' in json_data.keys():
        json_data = json_data['results']

        # Return None if there is no data in the result.
        if json_data == None:
            return None

        # Loop through each part of the multi-query
        for row in json_data:
            row_keys = row.keys()
            if 'stock' in row_keys or 'stats' in row_keys:
                data = format_basic_data(row)
            elif 'quote' in row_keys:
                # Now deal with "quote" which can be either quote data or dividend history, thanks to YQL for screwing that up.
                data = parse_quote_data(row)

            # Loop through each entry in the re-formated data to merge everything together
            for symbol in data:
                for entry in data[symbol]:
                    if symbol not in data_dict:
                        data_dict[symbol] = {}
                    data_dict[symbol][entry] = data[symbol][entry]


    else: #'results' not in json_data.keys()
        # No "result" key, so this isn't a multi-query

        # Return None if there is no data in the result.
        if json_data == None:
            return None

        if 'stock' in json_data.keys() or 'stats' in json_data.keys():
            data_dict = format_basic_data(json_data)

        elif 'quote' in json_data.keys():
            # Now deal with "quote" which can be either quote data or dividend history, thanks to YQL for screwing that up.
            data_dict = parse_quote_data(json_data)

    return data_dict



def parse_quote_data(data):
    # Now deal with "quote" which can be either quote data or dividend history, thanks to YQL for screwing that up.
    data_dict={}

    if type(data['quote']) == type(dict()):
        # This is a single row of either type

        if 'Dividends' in data['quote']:
            # This is a single dividend row
            data_dict = format_dividend_history_data(data)
        elif 'LastTradePriceOnly' in data['quote']:
            # This is a single quote row
            data_dict = format_basic_data(data)

    elif type(data['quote']) == type(list()):
        # This is multiple rows of either type
        if 'Dividends' in data['quote'][0]:
            # Process the list of dividend history data
            data_dict = format_dividend_history_data(data)
        elif 'LastTradePriceOnly' in data['quote'][0]:
            # Process the list of quote data
            data_dict = format_basic_data(data)
    else:
        raise Exception("'Quote' data is not in either quote or dividend history format.")

    return data_dict


def format_dividend_history_data(data):
    """
    Pass "dividend history" data not "basic data" or multi-queries)
    and it will put it into the standard dictionary format
    with each stock symbol being a dictionary entry and the data all properly
    associated with it
    :param data: "dividend history" data in one of these formats:
     {u'quote': [{u'Date': u'2014-02-06', u'Dividends': u'0.435710', u'Symbol': u'AAPL'}, {u... OR
     {u'quote': {u'Date': u'2014-11-06', u'Dividends': u'0.470000', u'Symbol': u'AAPL'}} OR
     {u'Date': u'2014-11-06', u'Dividends': u'0.470000', u'Symbol': u'AAPL'} OR
     [{u'Date': u'2014-11-06', u'Dividends': u'0.470000',...
    :return: Comes back as a dictionary in this format:
    {u'AAPL': {u'YearLow': u'70.5071'... etc...
    Stock symbol will always be upper case.
    """
    data_dict = {}
    # Get rid of the 'quote' at the front
    # You end up with either a list of dictionaries or a single dictionary
    if type(data) == type(dict()) and 'quote' in data.keys():
        try:
            data = data['quote']
        except:
            raise Exception("Dividend History data in wrong format.")


    if type(data) == type(dict()):
        # This is a single dividend history row, so it's not in a list
        symbol = data['Symbol'].upper()
        data_dict[symbol]['DividendHistory'] = []
        data_dict[symbol]['DividendHistory'].append(data)

    else:
        # This is a list of dividends
        # Dividend history is a list, so iterate through it and group symbols together
        list_iter = iter(data)
        row = next(list_iter,"eof")

        while row != "eof":
            current_symbol = row['Symbol'].upper()
            last_symbol = ""
            data_dict[current_symbol] = {}
            data_dict[current_symbol]['DividendHistory'] = []

            while row != "eof" and current_symbol == row['Symbol'].upper():
                data_dict[current_symbol]['DividendHistory'].append(row)
                last_symbol = current_symbol
                row = next(list_iter, "eof")

            # sort the list - is this necessary?
            data_dict[last_symbol]['DividendHistory'].sort(key=lambda x: datetime.datetime.strptime(x['Date'],"%Y-%m-%d"), reverse=True)

    return data_dict



def format_basic_data(data):
    """
    Pass "basic" data (i.e. "stock", "key stat", or "quote." NOT dividend history
    or multi-queries) and it will put it into the standard dictionary format
    with each stock symbol being a dictionary entry and the data all properly
    associated with it
    :param data: "basic" stock data in one of these formats:
     {u'quote': [{u'YearLow': u'70.5071',... OR
     {u'quote': {u'YearLow': u'70.5071', ... OR
     {u'YearLow': u'70.5071', ...
    :return: Comes back as a dictionary in this format:
    {u'AAPL': {u'YearLow': u'70.5071'... etc...
    Stock symbol will always be upper case.
    """

    data_dict = {}
    # Get rid of the 'quote', 'stat', etc at the front
    # You end up with either a list of dictionaries or a single dictionary
    if type(data) == type(dict()):
        data = data[data.keys()[0]]

    if type(data) == type(dict()):
        symbol = get_symbol(data)
        data_dict[symbol] = data

    # else if multiple rows come back, it returns it as a list
    else:
        for entry in data:
            symbol = get_symbol(entry)
            data_dict[symbol] = entry

    return data_dict


def get_symbol(data):
    """
    Called by format basic data. Takes data object in "basic format"
     and NOT a list and returns the symbol for it.
    :param data: basic data object in format:
    {u'quote': {u'YearLow': u'70.5071', ...
    :return: returns a stock symbol
    """
    symbol = ""
    try:
        symbol = data['symbol'].upper()
    except:
        try:
            symbol = data['sym'].upper()
        except:
            symbol = data['Symbol'].upper()

    if symbol == "":
        raise Exception("Could not find a stock symbol in data.")

    return symbol







# Retrieve the entire stock_list table from the database with all fields.
# Returns a list with a tuple for all rows with tuple in this format:
# (rowid, symbol, industry, sector, start, full_time_employees, has_dividends,
# last_dividend_date, last_updated).
# If the optional parameter is set to "", retrieve entire table, otherwise,
# retrieve just one symbol. There is also an optional parameter to specify a
# database name. This is only used for unit testing so that I don't have to
# destroy the existing database each time the test is ran.
def retrieve_stock_list_from_db(symbol = "", database="stocksdata.db"):

    db = sqlite.connect(os.path.dirname(__file__)+"\\"+database)
    cursor = db.cursor()
    if symbol == "":
        cursor.execute("select rowid, symbol, industry, sector, start, full_time_employees, has_dividends, last_dividend_date, last_updated from stock_list")
    else:
        cursor.execute("select rowid, symbol, industry, sector, start, full_time_employees, has_dividends, last_dividend_date, last_updated from stock_list where symbol='"+ symbol+"'")
    result = cursor.fetchall()
    db.close()
    return result





# Retrieve the entire dividend_history table from the database with all fields.
# Returns a list with a tuple for all rows with tuple in this format:
# (rowid, symbol, industry, sector, start, full_time_employees, has_dividends,
# last_dividend_date, last_updated).
# If the optional parameter is set to "", retrieve entire table, otherwise,
# retrieve just one symbol. There is also an optional parameter to specify a
# database name. This is only used for unit testing so that I don't have to
# destroy the existing database each time the test is ran.
def retrieve_dividend_history_from_db(stockid = "", database="stocksdata.db"):

    db = sqlite.connect(os.path.dirname(__file__)+"\\"+database)
    cursor = db.cursor()
    if stockid == "":
        cursor.execute("select rowid, stockid, Dividends, Date from dividend_history")
    else:
        cursor.execute("select rowid, stockid, Dividends, Date from dividend_history where stockid='"+ stockid +"'")
    result = cursor.fetchall()
    db.close()
    return result












# Takes a stock data object and stores the stocks and some of the stock data.
def store_stock_data(stock_data):
    if type(stock_data) != type(dict()):
        raise Exception("stock_data must be a dictionary.")

    # get the list of stocks currently in the database to compare to the passed list
    db = sqlite.connect(os.path.dirname(__file__)+"\\stocksdata.db")
    db_stock_data = db.execute('select symbol, industry,'\
                        + 'sector, start, full_time_employees, has_dividends, '\
                        + 'last_dividend_date, last_updated from stock_list')

    # Loop through each entry in the stock_data dictionary
    # and compare it to the existing data to determine what should be saved.

    # If it is not already in the database, do an insert.
    # If it is, then do an update.
    stored_symbols = [row[SYMBOL] for row in db_stock_data]
    insert_values = []
    update_values = []
    for symbol in stock_data.keys():
        # create list of new values to insert or update
        industry = stock_data[symbol]["Industry"]
        sector = stock_data[symbol]["Sector"]
        start = stock_data[symbol]["start"]
        full_time_employees = stock_data[symbol]["FullTimeEmployees"]
        if "DividendHistory" in stock_data[symbol] and len(stock_data[symbol]["DividendHistory"]) > 0:
            has_dividends = True
        else:
            has_dividends = False
        last_dividend_date = stock_data[symbol]["DividendDate"]
        last_updated = datetime.datetime.now()

        if symbol not in stored_symbols:
            insert_values.append((symbol, industry, sector, start, full_time_employees, has_dividends, last_dividend_date, last_updated))
        else:
            # create list for potential updates
            update_values.append((symbol, industry, sector, start, full_time_employees, has_dividends, last_dividend_date, last_updated))
            
    return insert_values, update_values




def get_quotes_data(symbol_list):    
    if type(symbol_list) != type(list()):
        raise Exception("symbol_list must be a list")

    # make a copy of the list so that we can re-run yql until we get the full list
    remaining_symbols = symbol_list[:]
    remaining_symbols = [symbol.upper() for symbol in remaining_symbols]
    result = {}
    final = {}

    # Since YQL fails a lot, do a loop until you get everything in the whole
    # original list of symbols
    while len(remaining_symbols) > 0:
        yql = "select "+ QUOTES_FIELDS +" from yahoo.finance.quotes where symbol in (" \
                        + '\'' \
                        + '\',\''.join(remaining_symbols) \
                        + '\'' \
                        + ")"

        result = execute_yql(yql)
        remaining_symbols = [symbol for symbol in remaining_symbols if symbol not in result.keys()]
        final = dict(final.items() + result.items())
        

    # If there is no PERatio returned, then it's a fair bet there won't be any
    # key stats for this quote
    for row in final:        
        if final[row]['PERatio'] == None:
            final[row]['NoKeyStats'] = True
        else:
            final[row]['NoKeyStats'] = False
        
    return final


    
def get_key_stats_data(symbol_list):

    if type(symbol_list) != type(list()):
        raise Exception("symbol_list must be a list")


    # make a copy of the list so that we can re-run yql until we get the full list
    remaining_symbols = symbol_list[:]
    remaining_symbols = [symbol.upper() for symbol in remaining_symbols]
    result = {}
    final = {}

    # Since YQL fails a lot, do a loop until you get everything in the whole
    # original list of symbols
    while len(remaining_symbols) > 0:
        yql = "select "+ KEY_STATS_FIELDS +" from yahoo.finance.keystats where symbol in (" \
                        + '\'' \
                        + '\',\''.join(remaining_symbols) \
                        + '\'' \
                        + ")"


        result = execute_yql(yql)
        remaining_symbols = [symbol for symbol in remaining_symbols if symbol not in result.keys()]
        final = dict(final.items() + result.items())

    #Reformat to make it easy to get data
    value = 0
    for symbol in final.keys():
        for key in final[symbol].keys():
            if type(final[symbol][key]) == type(dict()):
                try:
                    value = final[symbol][key]['content']
                except:
                    value = None
            else:                    
                value = final[symbol][key]

            final[symbol][key] = value
        #print str(symbol) + " - " + str(len(final[symbol]))


    return final




# Take a list of stock ticker symbols and return comprehensive stock data for those symbols
# in a standardized format
def get_combined_data(symbol_list):
    start_time = time.clock()
    
    quotes = get_quotes_data(symbol_list)
    stocks = get_stocks_data(symbol_list)
    key_stats = get_key_stats_data(symbol_list)
    div_hist = get_dividend_history_data(symbol_list)

    data = quotes

    for symbol in data.keys():
        
        for key in key_stats[symbol].keys():
            if key not in data:
                data[symbol][key] = key_stats[symbol][key]
            else:
                if data[symbol][key] != key_stats[symbol][key]:
                    raise Exception("Data in Quotes and Key Stats does not match: "\
                                + "Key="+key+"; Value Quotes="+data[symbol][key]+" Value Key Stats="+key_stats[symbol][key])

        for key in stocks[symbol].keys():
            if key not in data:
                data[symbol][key] = stocks[symbol][key]
            else:
                if data[symbol][key] != stocks[symbol][key]:
                    raise Exception("Data in Quotes and Stocks does not match: "\
                                + "Key="+key+"; Value Quotes="+data[symbol][key]+" Value Stocks="+stocks[symbol][key])

        if div_hist != None and symbol in div_hist:
            for key in div_hist[symbol].keys():
                if key not in data:
                    data[symbol][key] = div_hist[symbol][key]
                else:
                    if data[symbol][key] != div_hist[symbol][key]:
                        raise Exception("Data in Quotes and Stocks does not match: "\
                                    + "Key="+key+"; Value Quotes="+data[symbol][key]+" Value Stocks="+div_hist[symbol][key])

    standardize_data(data)

    end_time = time.clock()
    print str(end_time-start_time) + " seconds"
    
    return data




# Analyze data in various ways and label it. Input: data object with
# all stocks as output by get_combined_data
def analyze_data(data):
    # loop through each stock and analyze if it's a dividend stock or not
    for row in data:
        if is_dividend_stock(data[row]):
            data[row]['IsDividend'] = True
        else:
            data[row]['IsDividend'] = False


        print row + " - " + str(data[row]['IsDividend'])
            

    

# Pass this function a single row of standardized format stock data (i.e. that which comes out of
# get_combined_data() and it will determine if this is a dividend stock or not
def is_dividend_stock(stock_data_row):
    if (type(stock_data_row) != dict) or 'Symbol' not in stock_data_row:
        raise Exception("Parameter 'stock_data' must be a dictionary of data for a single stock")

    has_key_stats = not stock_data_row['NoKeyStats']
    forward_div = float(stock_data_row['ForwardAnnualDividendRate'])
    has_forward_div = not forward_div == 0.00 or forward_div == "N/A"
    has_div_hist = ('DividendHistory' in stock_data_row)
    error_string = "Bad data for Symbol: " + stock_data_row['Symbol'] + " - has_key_stats: " + str(has_key_stats) + "; forward_div: " + str(forward_div) + \
                   "; has_forward_div: " + str(has_forward_div) + "; has_div_hist: " + str(has_div_hist) + ". "

    div_share = float(stock_data_row['DividendShare'])
    div_date = stock_data_row['DividendDate']
    ex_div = stock_data_row['Ex_DividendDate']

    # key stats available
    if has_key_stats:
        if has_div_hist and has_forward_div:
            # This is a dividend stock with history and plan to pay another
            if (div_share > 0.00 and forward_div > 0.00 and div_date != None and ex_div != None):
                return True
            else:
                raise Exception(error_string + "This stock should have a dividend share, a forward dividend rate, a dividend date, and an ex dividend date.")

        elif not has_forward_div and has_div_hist:
            # This is supposed to be a former dividend stock that cut its dividend

            # Yahoo sometimes contains an old Ex_DividendDate / DividendDate and sometimes doesn't for stocks
            # that cut their dividends. So don't check Ex_DividendDate for this case.
            if (div_share == 0.00 and forward_div == 0.00):
                return False
            else:
                raise Exception(error_string + "This stock should not have a dividend share nor a forward dividend rate.")

        elif has_forward_div and not has_div_hist:
            # This is a stock that has no dividends in the past, but is forecasting one
            if (div_share == 0.00 and forward_div > 0.00 and div_date == None and ex_div == None):
                return True
            else:
                raise Exception(error_string + "This stock should not have a dividend share, dividend date, or ex dividend date, but should have a forward dividend rate.")

        else: # not has_forward_div and not has_div_hist:
            # This is a stock that has no dividend history nor is it forecasting one
            diff = datetime.timedelta(0)
            if (ex_div != None): diff = datetime.datetime.now() - ex_div
            if (div_share == 0.00 and forward_div == 0.00 and div_date == None and (ex_div == None or diff >= datetime.timedelta(10*365))):
                return False
            else:
                raise Exception(error_string + "This stock should not have a dividend share, forward dividend rate, or dividend date. "\
                        + "It should also not have an ex dividend rate or it should be older than 10 years old.")


    else: # No key stats available, so only have a dividend share and div history
        if stock_data_row['DividendShare'] > 0.00 and has_div_hist:
            return True
        elif not (stock_data_row['DividendShare'] > 0.00 and has_div_hist):
            return False
        else:
            raise Exception("Dividend Share and Div History does not match.")
                    




# A generic way for me to test out tables in real time. Requires that I
# include a symbol column
def get_any_data(symbol_list, table, fields="*"):

    if type(symbol_list) != type(list()):
        raise Exception("symbol_list must be a list")

    
    yql = "select "+ fields +" from "+table+" where symbol in (" \
                    + '\'' \
                    + '\',\''.join( symbol_list ) \
                    + '\'' \
                    + ")"
                    
    return execute_yql(yql)    











