# ****Data Layer****
# The Data Layer is responsible for obtaining data from either the web or the database
# It will also contain the database utilities necessary to create the database.

import urllib2
import json
import datetime
import time
import os
import csv
import logging



logging.basicConfig(filename="stocks.log",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)


__author__ = 'Bruce Nielson'

ALL_QUOTE_FIELDS = "YearLow, OneyrTargetPrice, DividendShare, ChangeFromFiftydayMovingAverage, FiftydayMovingAverage, "\
    +"SharesOwned, PercentChangeFromTwoHundreddayMovingAverage, PricePaid, DaysLow, DividendYield, Commission, "\
    +"EPSEstimateNextQuarter, ChangeFromYearLow, ChangeFromYearHigh, EarningsShare, AverageDailyVolume, LastTradePriceOnly, "\
    +"YearHigh, EBITDA, Change_PercentChange, AnnualizedGain, ShortRatio, LastTradeDate, PriceSales, EPSEstimateCurrentYear, "\
    +"BookValue, Bid, AskRealtime, PreviousClose, DaysRangeRealtime, EPSEstimateNextYear, Volume, HoldingsGainPercent, "\
    +"PercentChange, TickerTrend, Ask, ChangeRealtime, PriceEPSEstimateNextYear, HoldingsGain, Change, MarketCapitalization, "\
    +"Name, HoldingsValue, DaysRange, AfterHoursChangeRealtime, symbol, ChangePercentRealtime, DaysValueChange, LastTradeTime, "\
    +"StockExchange, DividendPayDate, LastTradeRealtimeWithTime, Notes, MarketCapRealtime, ExDividendDate, PERatio, "\
    +"DaysValueChangeRealtime, ErrorIndicationreturnedforsymbolchangedinvalid, ChangeinPercent, HoldingsValueRealtime, "\
    +"PercentChangeFromFiftydayMovingAverage, PriceBook, ChangeFromTwoHundreddayMovingAverage, DaysHigh, PercentChangeFromYearLow, "\
    +"TradeDate, LastTradeWithTime, BidRealtime, YearRange, HighLimit, OrderBookRealtime, HoldingsGainRealtime, PEGRatio, "\
    +"Currency, LowLimit, HoldingsGainPercentRealtime, TwoHundreddayMovingAverage, PERatioRealtime, PercebtChangeFromYearHigh, "\
    +"Open, PriceEPSEstimateCurrentYear, MoreInfo"

ALL_STOCK_FIELDS = "Sector, end, CompanyName, symbol, start, FullTimeEmployees, Industry" #"symbol, Industry, Sector, start, FullTimeEmployees"

ALL_DIVIDEND_HISTORY_FIELDS = "Symbol, Dividends, Date"


ALL_FIELDS = ALL_QUOTE_FIELDS + ", " + ALL_STOCK_FIELDS

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


def _process_symbol_list(symbol_list):
    if type(symbol_list) == type(str()) or type(symbol_list) == unicode:
        symbol_list = [symbol for symbol in symbol_list.split(", ")]

        #symbol_list = [symbol_list]
    if type(symbol_list) != type(list()):
        raise Exception("symbol_list must be a list") # pragma: no cover

    # make list all upper case
    symbol_list = [symbol.upper() for symbol in symbol_list]

    return symbol_list



# Load from CSV file all the data types yahoo will return
# So that its easy to build a dictionary for quotes
def load_yahoo_attributes():
    with open(os.path.dirname(__file__)+'\\yahooattributes.csv', 'rb') as f:
        reader = csv.reader(f)
        attributes = []
        for row in reader:
            attributes.append(row)

    return attributes


def list_to_str(li, include_comma=True):
    s = ""
    for item in li:
        if s == "":
            s = str(item)
        else:
            if (include_comma):
                s = s + "," + str(item)
            else:
                s = s + str(item)

    return s

# An alternative approach that might be faster?
def download_yahoo_quote_data(symbol_list):
    #url = "http://ichart.finance.yahoo.com/table.csv?s=AAPL&amp;d=1&amp;e=1&amp;f=2014&amp;g=d&amp;a=8&amp;b=7&amp;c=1984&amp;ignore=.csv" # Historical stock prices!

    # Get yahoo attributes dictionary
    attribute_list = load_yahoo_attributes()
    code_list = [row[2].strip("0") for row in attribute_list]
    attributes_str = list_to_str(code_list, False)
    attributes = [attribute[0] for attribute in attribute_list]

    #Convert symbol list to CSV
    sl = list_to_str(symbol_list)

    # Get Yahoo Financial Data
    url = "http://download.finance.yahoo.com/d/quotes.csv?s="+sl+"&f="+attributes_str+"=.csv"
    response = urllib2.urlopen(url)
    data = csv.reader(response)

    # Convert to dictionary in standard format
    stock_data = [row for row in data]

    result = {}
    for stock in stock_data:
        result[stock[0]] = {}
        for i in range(0,len(attribute_list)):
            value = {attribute_list[i][0]: stock[i]}
            result[stock[0]].update(value)

    return standardize_data(result, attributes)





# Given a list of symbols, get the "stocks" table data from Yahoo to
# store in the database. Returns in a standardized format.
def get_stock_data(symbol_list):
    """
    get_stock_data() takes a symbol list and returns Stocks table data for storing
    in the database.

    :param symbol_list: a list of stock ticker symbols in any case, i.e. ['AAPL', 'T', 'MSFT'] etc.
    :return: returns a data object that is a dictionary of stock symbols with the data as sub entries,
    i.e. {'AAPL': {Industry: 'Electronics', 'DividendHistory': {... dividend history}. It will contain
    "Stocks" table data.
    """

    def do_work(symbol_list):
        result = {}
        yql = "select "+ ALL_STOCK_FIELDS +" from yahoo.finance.stocks where symbol in (" \
                        + '\'' \
                        + '\',\''.join(symbol_list) \
                        + '\'' \
                        + ")"

        result = execute_yql(yql)

        return standardize_data(result, ALL_STOCK_FIELDS)

    symbol_list = _process_symbol_list(symbol_list)
    return _safe_get_data(do_work, symbol_list)





# From a list of symbols, get dividend history data. Returns in a
# standardized format.
def get_dividend_history_data(symbol_list):
    """
    get_dividend_history_data() takes a symbol list and returns Dividend History table data for storing
    in the database.

    :param symbol_list: a list of stock ticker symbols in any case, i.e. ['AAPL', 'T', 'MSFT'] etc.
    :return: returns a data object that is a dictionary of stock symbols with the data as sub entries,
    i.e. {'AAPL': {Industry: 'Electronics', 'DividendHistory': {... dividend history}. It will contain
    "Dividend History" table data.
    """
    def do_work(symbol_list):
        result = {}
        yql = "select "+ ALL_DIVIDEND_HISTORY_FIELDS +" from yahoo.finance.dividendhistory where"\
                        + " startDate = \"\" and endDate = \"\""\
                        + " and symbol in (" \
                        +'\'' \
                        + '\',\''.join(symbol_list) \
                        + '\'' \
                        + ")"

        # yql = yql % (today.year-10, today.month, today.day, today.year, today.month, today.day)
        result = execute_yql(yql)
        return standardize_dividend_history_data(result, ALL_DIVIDEND_HISTORY_FIELDS)


    symbol_list = _process_symbol_list(symbol_list)
    result = _safe_get_data(do_work, symbol_list)
    return result




def get_quote_data(symbol_list):
    """
    get_quote_data() takes a symbol list and returns Quote table data for storing
    in the database.

    :param symbol_list: a list of stock ticker symbols in any case, i.e. ['AAPL', 'T', 'MSFT'] etc.
    :return: returns a data object that is a dictionary of stock symbols with the data as sub entries,
    i.e. {'AAPL': {Industry: 'Electronics', 'DividendHistory': {... dividend history}. It will contain
    "Quote" table data.
    """
    def do_work(symbol_list):

        result = {}
        yql = "select "+ ALL_QUOTE_FIELDS +" from yahoo.finance.quotes where symbol in (" \
                        + '\'' \
                        + '\',\''.join(symbol_list) \
                        + '\'' \
                        + ")"

        result = execute_yql(yql)

        # If there is no PERatio returned, then it's a fair bet there won't be any
        # key stats for this quote
        for row in result:
            if result[row]['PERatio'] == None:
                result[row]['HasKeyStats'] = False
            else:
                result[row]['HasKeyStats'] = True

        return standardize_data(result, ALL_QUOTE_FIELDS)

    # By wrapping this in _safe_get_data it will repeat the YQL query until
    # every symbol has been loaded. This is because YQL is unreliable on
    # large datasets and just truncates them.
    symbol_list = _process_symbol_list(symbol_list)
    return _safe_get_data(do_work, symbol_list)









# Fix output so that there isn't such inconsistency in the data.
# i.e. "N/A" = 0.00 for a dividend, etc. This function can take any data
# set (quote, stock, key_stats) except Dividend History.
def standardize_data(data, fields):

    if type(fields) == type(str()):
        all_fields = fields.split(", ")
    elif type(fields) == type(list()):
        all_fields = fields
    else:
        raise Exception("List of fields must be csv string or list")

    for row in data:
        for item in all_fields:

            # If item is not in this quote, then fill in with default data
            if item not in data[row]:
                data[row][item] = None

            value = data[row][item]


            if value == None or value == "N/A" or value == "None":
                data[row][item] = None

            #if item is dollar, integer or decimal
            elif item in ['LastTradePriceOnly', 'YearLow', 'YearHigh', 'DividendShare', 'EarningsShare',\
                        'PERatio', 'PriceSales', 'PEGRatio', 'ShortRatio', 'BookValue', 'PriceBookTotalDebt', \
                        'ReturnOnEquity', 'TrailingPE', 'RevenuePerShare', 'MarketCap','PriceBook', 'EBITDA', \
                        'OperatingCashFlow', 'Beta', 'ReturnonAssests', 'ForwardAnnualDividendRate', \
                        'SharesShort', 'CurrentRatio', 'BookValuePerShare', 'TotalCashPerShare', 'TotalCash', \
                        'Revenue', 'ForwardPE', 'DilutedEPS', 'SharesOutstanding', 'TotalDebtEquity', \
                        'FullTimeEmployees', 'TotalDebt', 'MarketCapitalization']:

                if type(value) == type(float()):
                    pass
                else:
                    try:
                        data[row][item] = convert_to_float(value)
                    except ValueError: # pragma: no cover
                        raise Exception("For "+row+", "+item+": "+value+" is not a valid value.")


                value = str(value).replace(",","")


                if value == "N/A" or value == None or value == "None":
                    data[row][item] = 0.00
                elif type(value) == type(float):
                    data[row][item] = float(value)
                elif _is_number(value):
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
            elif item in ['Ex_DividendDate', 'start', 'DividendDate', 'ExDividendDate', 'DividendPayDate', 'LastTradeDate', 'TradeDate']:

                if type(value) == datetime.datetime:
                    pass
                elif value == "N/A" or value == None or value == "None" or ((type(value) == type(str()) or type(value) == type(unicode())) and "NaN" in value):
                    data[row][item] = None
                else:
                    if isinstance(data[row][item], datetime.datetime):
                        pass
                    else:
                        try:
                            data[row][item] = convert_to_date(data[row][item])
                        except (TypeError, ValueError) as e: # pragma: no cover
                            raise Exception("For "+row+", "+item+": "+value+" Incorrect data format for a date. Should be YYYY-MM-DD.")

            # if item is a percentage
            elif item[:7] == 'Percent' or item[-7:] == "Percent" or (value == type(str()) and value[-1:] == "%"):

                if type(value) == type(float()):
                    pass
                #elif value == "N/A" or value == None or value == "None":
                #    data[row][item] = 0.0
                else:
                    try:
                        data[row][item] = float(value.strip('%').replace(",","")) / 100.0
                    except (TypeError, ValueError): # pragma: no cover
                        pass

            else:
                # Try other conversions
                try:
                    data[row][item] = convert_to_float(value)
                except (TypeError, ValueError): # pragma: no cover
                    pass

    return data




# Fix output so that there isn't such inconsistency in the data.
# i.e. "N/A" = 0.00 for a dividend, etc. This function takes only dividend history data.
def standardize_dividend_history_data(div_history_data, fields):

    div_hist_fields = fields.split(", ")

    for symbol in div_history_data:
        dividend_list =  div_history_data[symbol]['DividendHistory']
        for div in dividend_list:
            for field in div_hist_fields:
                # If field is not in this quote, then fill in with default data
                if field not in div:
                    div[field] = None

                #if item is dollar, integer or decimal
                if field in ['Dividends']:
                    if type(div[field]) == type(float()):
                        pass
                    else:
                        try:
                            div[field] = convert_to_float(div[field])
                        except ValueError: # pragma: no cover
                            raise ValueError("For "+symbol+", "+field+": "+str(div[field])+" is not a valid value.")


                # if item is a date
                if field in ['Date']:
                    if type(div[field]) == datetime.datetime:
                        pass
                    else:
                        try:
                            div[field] = convert_to_date(div[field])
                        except ValueError: # pragma: no cover
                            raise ValueError("For "+symbol+", "+field+": "+str(div[field])+" Incorrect data format for a date. Should be YYYY-MM-DD or MM/DD/YYYY.")

    div_list = []
    for symbol in div_history_data:
        for div in div_history_data[symbol]['DividendHistory']:
            if div['Dividends'] != 0.0:
                div_list.append(div)
        div_history_data[symbol]['DividendHistory'] = div_list
        div_list = []

    return div_history_data




# Attempt to convert value to a float (decimal) format or else raise a
# ValueError exception
def convert_to_float(value):
    value = str(value).replace(",","")

    if value == "N/A" or value == None or value == "None" or value == "--" or value == '-':
        return 0.00
    elif value[-1:] == "%":
        return float(value.strip('%').replace(",","")) / 100.0
    elif type(value) == type(float):
        return float(value)
    elif _is_number(value):
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
    if type(value) == datetime.datetime:
        return value
    elif value == "N/A" or value == None or value == "None" or ((type(value) == type(str()) or type(value) == type(unicode())) and "NaN" in value):
        return None
    else:
        try:
            return datetime.datetime.strptime(value,"%Y-%m-%d")
        except (ValueError, TypeError) as e:
            try:
                return datetime.datetime.strptime(value,"%m/%d/%Y")
            except (ValueError, TypeError) as e:
                try:
                    return datetime.datetime.strptime(value,"%b %d, %Y")
                except (ValueError, TypeError) as e: # pragma: no cover
                    print value
                    raise e




# Pass a string and return True if it can be converted to a float without
# an exception
def _is_number(s):
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

    def _parse_quote_data(data):
        # Now deal with "quote" which can be either quote data or dividend history, thanks to YQL for screwing that up.
        data_dict={}
        if data == None:
            return data_dict

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
            raise Exception("'Quote' data is not in either quote or dividend history format.") # pragma: no cover

        return data_dict

    #print yql
    url = "http://query.yahooapis.com/v1/public/yql?q=" \
            + urllib2.quote(yql) \
            + "&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&format=json"
            #"&format=json&env=http%3A%2F%2Fdatatables.org%2Falltables.env&callback="

    try:
        result = urllib2.urlopen(url)
    except urllib2.HTTPError, e: # pragma: no cover
        raise Exception("HTTP error: ", e.code, e.reason)
    except urllib2.URLError, e:
        raise Exception("Network error: ", e.reason)

    # Couldn't get mock tests to work without this
    if result == "mock file object":
        result = json.loads("")
    else:
        result = json.loads(result.read()) # pragma: no cover

    #print result

    json_data = result['query']['results']

    # Return None if there is no data in the result.
    if json_data == None:
        return {}

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

        # Loop through each part of the multi-query
        for row in json_data:
            row_keys = row.keys()
            if 'stock' in row_keys or 'stats' in row_keys:
                data = format_basic_data(row)
            elif 'quote' in row_keys:
                # Now deal with "quote" which can be either quote data or dividend history, thanks to YQL for screwing that up.
                data = _parse_quote_data(row)

            # Loop through each entry in the re-formated data to merge everything together
            for symbol in data:
                for entry in data[symbol]:
                    if symbol not in data_dict:
                        data_dict[symbol] = {}
                    data_dict[symbol][entry] = data[symbol][entry]


    else: #'results' not in json_data.keys()
        # No "result" key, so this isn't a multi-query

        if 'stock' in json_data.keys() or 'stats' in json_data.keys():
            data_dict = format_basic_data(json_data)

        elif 'quote' in json_data.keys():
            # Now deal with "quote" which can be either quote data or dividend history, thanks to YQL for screwing that up.
            data_dict = _parse_quote_data(json_data)

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
    if (data == None):
        return data_dict

    # Get rid of the 'quote' at the front
    # You end up with either a list of dictionaries or a single dictionary
    if type(data) == type(dict()) and 'quote' in data.keys():
        try:
            data = data['quote']
        except:
            raise Exception("Dividend History data in wrong format.") # pragma: no cover


    if type(data) == type(dict()):
        # This is a single dividend history row, so it's not in a list
        symbol = data['Symbol'].upper()
        data_dict[symbol] = {}
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
            try:
                data_dict[last_symbol]['DividendHistory'].sort(key=lambda x: datetime.datetime.strptime(x['Date'],"%Y-%m-%d"), reverse=True)
            except TypeError:
                data_dict[last_symbol]['DividendHistory'].sort(key=lambda x: x['Date'], reverse=True)
            except KeyError:
                raise # ToDo: I keep getting a key error here when YQL fails to come back with a date. Deal with this more gracefully

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
    if data == None:
        return data_dict

    # Get rid of the 'quote', 'stat', etc at the front
    # You end up with either a list of dictionaries or a single dictionary
    if type(data) == type(dict()):
        data = data[data.keys()[0]]

    if type(data) == type(dict()):
        symbol = _get_symbol(data)
        data_dict[symbol] = data

    # else if multiple rows come back, it returns it as a list
    else:
        for entry in data:
            symbol = _get_symbol(entry)
            data_dict[symbol] = entry

    return data_dict


def _get_symbol(data):
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













# Because YQL fails to bring everything back on a large data set
# this function will take any "get data" function, i.e.
# get_quote_data(symbol_list), get_key_stats_data(symbol_list)
# etc. and a symbol_list and it will do the work of
# calling it multiple times until the result is everything
def _safe_get_data(function, symbol_list):


    # make a copy of the list so that we can re-run yql until we get the full list
    remaining_symbols = symbol_list[:]
    remaining_symbols = [symbol.upper() for symbol in remaining_symbols]
    starting_count = len(remaining_symbols)
    final = {}

    # Try once with everything
    result = function(remaining_symbols)
    if len(result) > 0:
        remaining_symbols = [symbol for symbol in remaining_symbols if symbol not in result.keys()]
        final = dict(final.items() + result.items())

    # If it didn't process everything in one try, then split it up and do it a little at a time
    all_not_processed = []
    finished = False
    chunk_size = 25
    while not finished:

        while len(remaining_symbols) > 0:

            chunk = remaining_symbols[0:chunk_size]
            result = function(chunk)
            if len(result) > 0:
                final = dict(final.items() + result.items())

            not_processed = [symbol for symbol in chunk if symbol not in result.keys()]
            if len(not_processed) > 0:
                logging.info("Not Processed:" + str(len(not_processed)))

            # Drop that processed items
            all_not_processed = all_not_processed + [symbol for symbol in not_processed]
            remaining_symbols = remaining_symbols[chunk_size:]

        # Repeat with a smaller chuck size with whatever is remaining
        if chunk_size == 25:
            remaining_symbols = all_not_processed[:]
            all_not_processed = []
            chunk_size = 1
        elif chunk_size == 1:
            finished = True

    # Take all the not processed items and run them through one more time just to be sure
    result = function(all_not_processed)
    if len(result) > 0:
        not_processed = [symbol for symbol in all_not_processed if symbol not in result.keys()]
        all_not_processed = [symbol for symbol in not_processed]
        final = dict(final.items() + result.items())

    logging.info("Total Not Processed: " + str(len(all_not_processed)))
    logging.info(str(all_not_processed))
    #print len(all_not_processed)
    return final





# Take a list of stock ticker symbols and return comprehensive stock data for those symbols
# in a standardized format
def get_combined_data(symbol_list):
    start_time = time.clock()

    quotes = get_quote_data(symbol_list)
    stocks = get_stock_data(symbol_list)
    div_hist = get_dividend_history_data(symbol_list)

    data = quotes

    for symbol in data.keys():

        for key in stocks[symbol].keys():
            if key not in data[symbol]:
                data[symbol][key] = stocks[symbol][key]
            else:
                if str(data[symbol][key]) != str(stocks[symbol][key]):
                    raise Exception("Data in Quotes and Stocks does not match: "\
                                + "Key="+str(key)+"; Value Quotes="+str(data[symbol][key])+" Value Stocks="+str(stocks[symbol][key]))

        if div_hist != None and symbol in div_hist:
            for key in div_hist[symbol].keys():
                if key not in data[symbol]:
                    data[symbol][key] = div_hist[symbol][key]
                else:
                    if str(data[symbol][key]) != str(div_hist[symbol][key]):
                        raise Exception("Data in Quotes and Stocks does not match: "\
                                    + "Key="+str(key)+"; Value Quotes="+str(data[symbol][key])+" Value Stocks="+str(div_hist[symbol][key]))

    standardize_data(data, ALL_FIELDS)

    end_time = time.clock()
    logging.info("get_combined_data: " + str(end_time-start_time) + " seconds")

    return data







def create_test_data(): # pragma: no cover
    import os
    import pickle

    def pickle_test_data(data):
        os.remove(os.path.dirname(__file__)+"\\testdata.txt")
        f = open(os.path.dirname(__file__)+"\\"+'testdata.txt', 'w')
        pickle.dump(data, f)


    def quote_yql(symbol_list):
        symbol_list = _process_symbol_list(symbol_list)
        yql = "select "+ ALL_QUOTE_FIELDS +" from yahoo.finance.quotes where symbol in (" \
                + '\'' \
                + '\',\''.join(symbol_list) \
                + '\'' \
                + ")"
        return yql

    def stock_yql(symbol_list):
        symbol_list = _process_symbol_list(symbol_list)
        yql = "select "+ ALL_STOCK_FIELDS +" from yahoo.finance.stocks where symbol in (" \
            + '\'' \
            + '\',\''.join(symbol_list) \
            + '\'' \
            + ")"
        return yql

    def div_hist_yql(symbol_list):
        symbol_list = _process_symbol_list(symbol_list)
        yql = "select "+ ALL_DIVIDEND_HISTORY_FIELDS +" from yahoo.finance.dividendhistory where"\
            + " startDate = \"\" and endDate = \"\""\
            + " and symbol in (" \
            +'\'' \
            + '\',\''.join(symbol_list) \
            + '\'' \
            + ")"
        return yql

    def stock_div_yql(symbol_list):
        symbol_list = _process_symbol_list(symbol_list)
        yql = "select * from yql.query.multi where queries = \""\
            + "SELECT "+ALL_STOCK_FIELDS+" FROM yahoo.finance.stocks WHERE symbol in ("\
            + '\'' \
            + '\',\''.join(symbol_list) \
            + '\'' \
            + "); " \
            + "SELECT "+ALL_DIVIDEND_HISTORY_FIELDS+" FROM yahoo.finance.dividendhistory WHERE "\
             + "startDate = \'\' and endDate = \'\' and symbol in (" \
            + '\'' \
            + '\',\''.join(symbol_list) \
            + '\'' \
            + ")\""
        return yql


    def create_fake_data(yql):
        url = "http://query.yahooapis.com/v1/public/yql?q=" \
            + urllib2.quote(yql) \
            + "&format=json&env=http%3A%2F%2Fdatatables.org%2Falltables.env&callback="

        try:
            result = urllib2.urlopen(url)
        except urllib2.HTTPError, e:
            raise Exception("HTTP error: ", e.code, e.reason)
        except urllib2.URLError, e:
            raise Exception("Network error: ", e.reason)

        return json.loads(result.read())


    data = {}

    print "Creating symbol list test data:"
    sl = ['aapl', 'T', 'MSFT', 'GOOG']
    data['q1r'] = get_quote_data(sl)
    data['q1t'] = create_fake_data(quote_yql(sl))
    print "completed quote..."

    data['s1r'] = get_stock_data(sl)
    data['s1t'] = create_fake_data(stock_yql(sl))
    print "completed stocks..."

    sl = ['aapl', 'T', 'MSFT']
    data['div1r'] = get_dividend_history_data(['aapl', 'T', 'MSFT'])
    data['div1t'] = create_fake_data(div_hist_yql(sl))
    print "completed dividend history..."

    print "Creating single symbol test data:"
    sl = ['aapl']
    data['q2r'] = get_quote_data(sl)
    data['q2t'] = create_fake_data(quote_yql(sl))

    print "completed quote..."
    data['s2r'] = get_stock_data(sl)
    data['s2t'] = create_fake_data(stock_yql(sl))
    print "completed stock..."
    data['div3r'] = get_dividend_history_data(sl)
    data['div3t'] = create_fake_data(div_hist_yql(sl))
    data['div4r'] = get_dividend_history_data('cnhi')
    data['div4t'] = create_fake_data(div_hist_yql('cnhi'))
    print "completed dividend history..."
    #data['div3t'] = get_dividend_history_data(sl)
    #data['div3r'] = create_fake_data(quote_yql(sl))

    print "Creating no dividends example:"
    sl = ['GOOG']
    data['div2r'] = get_dividend_history_data(sl)
    data['div2t'] = create_fake_data(div_hist_yql(sl))
    print "completed no dividends..."

    #data['any'] = get_any_data(['aapl'], "yahoo.finance.quotes", "LastTradePriceOnly, Symbol, DividendShare")

    pickle_test_data(data)






