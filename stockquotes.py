# ****Data Layer****
# The Data Layer is responsible for obtaining data from either the web or the database
# It will also contain the database utilities necessary to create the database.

import urllib2
import json
import datetime
import time


__author__ = 'Bruce Nielson'

ALL_KEY_STATS_FIELDS = "symbol, TotalDebt, ReturnOnEquity, TrailingPE, RevenuePerShare, MarketCap, " \
         + "PriceBook, EBITDA, PayoutRatio, OperatingCashFlow, Beta, ReturnonAssests, "\
         + "TrailingAnnualDividendYield, ForwardAnnualDividendRate, p_5YearAverageDividentYield, "\
         + "DividendDate, Ex_DividendDate, ForwardAnnualDividendYield, SharesShort, "\
         + "CurrentRatio, BookValuePerShare, ProfitMargin, TotalCashPerShare, QtrlyEarningsGrowth, "\
         + "TotalCash, Revenue, ForwardPE, DilutedEPS, OperatingMargin, SharesOutstanding, "\
         + "TotalDebtEquity"


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
    +"Open, PriceEPSEstimateCurrentYear, MoreInfo, Symbol"

ALL_STOCK_FIELDS = "Sector, end, CompanyName, symbol, start, FullTimeEmployees, Industry" #"symbol, Industry, Sector, start, FullTimeEmployees"

ALL_DIVIDEND_HISTORY_FIELDS = "Symbol, Dividends, Date"


ALL_FIELDS = ALL_QUOTE_FIELDS + ", " + ALL_KEY_STATS_FIELDS + ", " + ALL_STOCK_FIELDS

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

    def do_work(symbol_list):
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

        #yql = yql % (today.year-10, today.month, today.day, today.year, today.month, today.day)
        result = execute_yql(yql)
        result = standardize_dividend_history_data(result, ALL_DIVIDEND_HISTORY_FIELDS)
        result = standardize_data(result, ALL_STOCK_FIELDS)
        return result


    symbol_list = __process_symbol_list(symbol_list)
    return __safe_get_data(do_work, symbol_list)


def __process_symbol_list(symbol_list):
    if type(symbol_list) == type(str()):
        symbol_list = [symbol for symbol in symbol_list.split(", ")]

        #symbol_list = [symbol_list]
    if type(symbol_list) != type(list()):
        raise Exception("symbol_list must be a list") # pragma: no cover

    # make list all upper case
    symbol_list = [symbol.upper() for symbol in symbol_list]

    return symbol_list




# Given a list of symbols, get the "stocks" data from Yahoo to
# store in the database. Returns in a standardized format.
def get_stock_data(symbol_list):

    def do_work(symbol_list):
        result = {}
        yql = "select "+ ALL_STOCK_FIELDS +" from yahoo.finance.stocks where symbol in (" \
                        + '\'' \
                        + '\',\''.join(symbol_list) \
                        + '\'' \
                        + ")"

        result = execute_yql(yql)

        return standardize_data(result, ALL_STOCK_FIELDS)

    symbol_list = __process_symbol_list(symbol_list)
    return __safe_get_data(do_work, symbol_list)



# From a list of symbols, get dividend history data. Returns in a
# standardized format.
def get_dividend_history_data(symbol_list):

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


    symbol_list = __process_symbol_list(symbol_list)
    result = __safe_get_data(do_work, symbol_list)
    return result



# Fix output so that there isn't such inconsistency in the data.
# i.e. "N/A" = 0.00 for a dividend, etc. This function can take any data
# set (quote, stock, key_stats) except Dividend History.
def standardize_data(data, fields):
    all_fields = fields.split(", ")
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
                if type(value) == type(float()):
                    pass
                else:
                    try:
                        data[row][item] = __convert_to_float(value)
                    except ValueError: # pragma: no cover
                        raise Exception("For "+row+", "+item+": "+value+" is not a valid value.")


                value = data[row][item]
                value = str(value).replace(",","")

                """
                if value == "N/A" or value == None or value == "None":
                    data[row][item] = 0.00
                elif type(value) == type(float):
                    data[row][item] = float(value)
                elif __is_number(value):
                    data[row][item] = float(value)
                elif value[len(value)-1] == "M":
                    data[row][item] = float(value[:len(value)-1])*1000000.00
                elif value[len(value)-1] == "B":
                    data[row][item] = float(value[:len(value)-1])*1000000000.00
                elif value[len(value)-1] == "T":
                    data[row][item] = float(value[:len(value)-1])*1000000000000.00
                else:
                    raise Exception("For "+row+", "+item+": "+value+" is not a valid value.")
                """

            # if item is a date
            elif item in ['Ex_DividendDate', 'start', 'DividendDate']:
                value = data[row][item]

                if value == "N/A" or value == None or value == "None" or "NaN" in value:
                    data[row][item] = None
                else:
                    if type(data[row][item]) == type(datetime.datetime):
                        pass
                    else:
                        try:
                            data[row][item] = __convert_to_date(data[row][item])
                        except ValueError: # pragma: no cover
                            raise ValueError("For "+row+", "+item+": "+value+" Incorrect data format for a date. Should be YYYY-MM-DD.")

            # if item is a percentage
            elif item in ['QtrlyEarningsGrowth', 'PayoutRatio', 'ProfitMargin', 'TrailingAnnualDividendYield',\
                        'ForwardAnnualDividendYield', 'p_5YearAverageDividentYield', 'OperatingMargin']:

                value = data[row][item]

                if type(value) == type(float()):
                    pass
                elif value == "N/A" or value == None or value == "None":
                    data[row][item] = 0.0
                else:
                    try:
                        data[row][item] = float(value.strip('%').replace(",","")) / 100.0
                    except ValueError: # pragma: no cover
                        raise ValueError("For "+row+", "+item+": "+value+" is not a valid value.")

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
                    if type(field) == type(float()):
                        pass
                    else:
                        try:
                            div[field] = __convert_to_float(div[field])
                        except ValueError: # pragma: no cover
                            raise ValueError("For "+symbol+", "+field+": "+str(div[field])+" is not a valid value.")

                # if item is a date
                if field in ['Date']:
                    if type(field) == type(datetime.datetime):
                        pass
                    else:
                        try:
                            div[field] = __convert_to_date(div[field])
                        except ValueError: # pragma: no cover
                            raise ValueError("For "+symbol+", "+field+": "+str(div[field])+" Incorrect data format for a date. Should be YYYY-MM-DD.")

    return div_history_data




# Attempt to convert value to a float (decimal) format or else raise a
# ValueError exception
def __convert_to_float(value):
    value = str(value).replace(",","")

    if value == "N/A" or value == None or value == "None":
        return 0.00
    elif type(value) == type(float):
        return float(value)
    elif __is_number(value):
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
def __convert_to_date(value):
    if value == "N/A" or value == None or value == "None" or "NaN" in value:
        return None
    else:
        try:
            return datetime.datetime.strptime(value,"%Y-%m-%d")
        except ValueError:
            try:
                return datetime.datetime.strptime(value,"%b %d, %Y")
            except ValueError: # pragma: no cover
                raise ValueError()




# Pass a string and return True if it can be converted to a float without
# an exception
def __is_number(s):
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

    def __parse_quote_data(data):
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
            + "&format=json&env=http%3A%2F%2Fdatatables.org%2Falltables.env&callback="

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
                data = __parse_quote_data(row)

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
            data_dict = __parse_quote_data(json_data)

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
    if data == None:
        return data_dict

    # Get rid of the 'quote', 'stat', etc at the front
    # You end up with either a list of dictionaries or a single dictionary
    if type(data) == type(dict()):
        data = data[data.keys()[0]]

    if type(data) == type(dict()):
        symbol = __get_symbol(data)
        data_dict[symbol] = data

    # else if multiple rows come back, it returns it as a list
    else:
        for entry in data:
            symbol = __get_symbol(entry)
            data_dict[symbol] = entry

    return data_dict


def __get_symbol(data):
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













def get_quote_data(symbol_list):

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

    # By wrapping this in __safe_get_data it will repeat the YQL query until
    # every symbol has been loaded. This is because YQL is unreliable on
    # large datasets and just truncates them.
    symbol_list = __process_symbol_list(symbol_list)
    return __safe_get_data(do_work, symbol_list)




# Because YQL fails to bring everything back on a large data set
# this function will take any "get data" function, i.e.
# get_quote_data(symbol_list), get_key_stats_data(symbol_list)
# etc. and a symbol_list and it will do the work of
# calling it multiple times until the result is everything
def __safe_get_data(function, symbol_list):


    # make a copy of the list so that we can re-run yql until we get the full list
    remaining_symbols = symbol_list[:]
    remaining_symbols = [symbol.upper() for symbol in remaining_symbols]
    final = {}

    while len(remaining_symbols) > 0:
        result = function(remaining_symbols)
        if len(result) > 0:
            remaining_symbols = [symbol for symbol in remaining_symbols if symbol not in result.keys()]
        else:
            remaining_symbols = {}
        final = dict(final.items() + result.items())

    return final




def get_key_stats_data(symbol_list):

    def do_work(symbol_list):
        result = {}
        yql = "select "+ ALL_KEY_STATS_FIELDS +" from yahoo.finance.keystats where symbol in (" \
                        + '\'' \
                        + '\',\''.join(symbol_list) \
                        + '\'' \
                        + ")"


        result = execute_yql(yql)

        #Reformat to make it easy to get data
        value = 0
        for symbol in result.keys():
            for key in result[symbol].keys():
                if type(result[symbol][key]) == type(dict()):
                    try:
                        value = result[symbol][key]['content']
                    except:
                        value = None
                else:
                    value = result[symbol][key]

                result[symbol][key] = value
            #print str(symbol) + " - " + str(len(final[symbol]))


        return standardize_data(result, ALL_KEY_STATS_FIELDS)

    symbol_list = __process_symbol_list(symbol_list)
    return __safe_get_data(do_work, symbol_list)



# Take a list of stock ticker symbols and return comprehensive stock data for those symbols
# in a standardized format
def get_combined_data(symbol_list):
    start_time = time.clock()
    
    quotes = get_quote_data(symbol_list)
    stocks = get_stock_data(symbol_list)
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

    standardize_data(data, ALL_FIELDS)

    end_time = time.clock()
    print str(end_time-start_time) + " seconds"
    
    return data





# A generic way for me to test out tables in real time. Requires that I
# include a symbol column
def get_any_data(symbol_list, table, fields="*"):

    symbol_list = __process_symbol_list(symbol_list)
    
    yql = "select "+ fields +" from "+table+" where symbol in (" \
                    + '\'' \
                    + '\',\''.join( symbol_list ) \
                    + '\'' \
                    + ")"
                    
    return execute_yql(yql)    






def create_data(): # pragma: no cover
    import os
    import pickle

    def pickle_test_data(data):
        os.remove(os.path.dirname(__file__)+"\\testdata.txt")
        f = open(os.path.dirname(__file__)+"\\"+'testdata.txt', 'w')
        pickle.dump(data, f)


    def quote_yql(symbol_list):
        symbol_list = __process_symbol_list(symbol_list)
        yql = "select "+ ALL_QUOTE_FIELDS +" from yahoo.finance.quotes where symbol in (" \
                + '\'' \
                + '\',\''.join(symbol_list) \
                + '\'' \
                + ")"
        return yql

    def stock_yql(symbol_list):
        symbol_list = __process_symbol_list(symbol_list)
        yql = "select "+ ALL_STOCK_FIELDS +" from yahoo.finance.stocks where symbol in (" \
            + '\'' \
            + '\',\''.join(symbol_list) \
            + '\'' \
            + ")"
        return yql

    def key_stat_yql(symbol_list):
        symbol_list = __process_symbol_list(symbol_list)
        yql = "select "+ ALL_KEY_STATS_FIELDS +" from yahoo.finance.keystats where symbol in (" \
            + '\'' \
            + '\',\''.join(symbol_list) \
            + '\'' \
            + ")"
        return yql

    def div_hist_yql(symbol_list):
        symbol_list = __process_symbol_list(symbol_list)
        yql = "select "+ ALL_DIVIDEND_HISTORY_FIELDS +" from yahoo.finance.dividendhistory where"\
            + " startDate = \"\" and endDate = \"\""\
            + " and symbol in (" \
            +'\'' \
            + '\',\''.join(symbol_list) \
            + '\'' \
            + ")"
        return yql

    def stock_div_yql(symbol_list):
        symbol_list = __process_symbol_list(symbol_list)
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


    data['ks1r'] = get_key_stats_data(sl)
    data['ks1t'] = create_fake_data(key_stat_yql(sl))
    print "completed key stats..."
    data['s1r'] = get_stock_data(sl)
    data['s1t'] = create_fake_data(stock_yql(sl))
    print "completed stocks..."

    sl = ['aapl', 'T', 'MSFT']
    data['div1r'] = get_dividend_history_data(['aapl', 'T', 'MSFT'])
    data['div1t'] = create_fake_data(div_hist_yql(sl))
    print "completed dividend history..."
    data['sd1r'] = get_stock_and_dividend_history_data(['aapl', 'T', 'MSFT'])
    data['sd1t'] = create_fake_data(stock_div_yql(sl))
    print "completed stock and dividend history..."


    print "Creating single symbol test data:"
    sl = ['aapl']
    data['q2r'] = get_quote_data(sl)
    data['q2t'] = create_fake_data(quote_yql(sl))
    print "completed quote..."
    data['ks2r'] = get_key_stats_data(sl)
    data['ks2t'] = create_fake_data(key_stat_yql(sl))
    print "completed key stat..."
    data['s2r'] = get_stock_data(sl)
    data['s2t'] = create_fake_data(stock_yql(sl))
    print "completed stock..."
    data['sd2r'] = get_stock_and_dividend_history_data(sl)
    data['sd2t'] = create_fake_data(stock_div_yql(sl))
    print "completed stock and dividend history..."
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







