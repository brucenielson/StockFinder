import urllib2
import json
import datetime
import time
import lxml.html
import sqlite3 as sqlite

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


def create_database():
    # Create or open the database
    conn = sqlite.connect("stocksdata.db")

    # create the tables
    conn.execute('create table stocklist(symbol, industry,'\
                        + 'sector, start, full_time_employees, has_dividends, '\
                        + 'last_dividend_date, last_updated)')
    conn.execute('create table dividend_history (stockid, dividends, date)')
    conn.execute('create table label(stockid, label)')
    conn.execute('create table url(stockid, url)')
    conn.execute('create index symbolx on stocklist(symbol)')
    conn.execute('create index labelx on label(label)')

    conn.close()



def obtain_parse_wiki_snp500():
    """Download and parse the Wikipedia list of S&P500 
    constituents using requests and libxml.

    Returns a list of tuples for to add to MySQL."""

    # Stores the current time, for the created_at record
    now = datetime.datetime.utcnow()

    # Use libxml to download the list of S&P500 companies and obtain the symbol table
    page = lxml.html.parse('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    symbolslist = page.xpath('//table[1]/tr/td[1]/a/text()')

    return symbolslist




def execute_yql(yql):
    url = "http://query.yahooapis.com/v1/public/yql?q=" \
            + urllib2.quote(yql) \
            + "&format=json&env=http%3A%2F%2Fdatatables.org%2Falltables.env&callback="

    try: 
        result = urllib2.urlopen(url)
    except urllib2.HTTPError, e:        
        raise Exception("HTTP error: ", e.code, e.reason);
    except urllib2.URLError, e:
        raise Exception("Network error: ", e.reason);
           
    result = json.loads(result.read())

    json_data = result['query']['results']
    
    if json_data == None:
        return None

    else:
        json_data = json_data[json_data.keys()[0]]
        data_dict = {}
        
        if type(json_data) == type(list()) and 'Dividends' in json_data[0] and 'Date' in json_data[0]:
            # Divident history is a list, so iterate through it and group symbols together
            list_iter = iter(json_data)
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
            
        # otherwise this is quote, stock, or stats data
        else:
        
            # If only one row is returned, it comes back as a dictionary
            if type(json_data) == type(dict()):
                try:
                    data_dict[json_data['symbol'].upper()] = json_data                            
                except:
                    try:
                        data_dict[json_data['Symbol'].upper()] = json_data
                    except:
                        data_dict[json_data['sym'].upper()] = json_data
            # else if multiple rows come back, it returns it as a list
            else:
                for entry in json_data:
                    symbol = ""
                    try:
                        symbol = entry['symbol'].upper()
                    except:
                        try:
                            symbol = entry['sym'].upper()
                        except:
                            symbol = entry['Symbol'].upper()

                    data_dict[symbol] = entry
    
        return data_dict




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
    

                    
    return final




# Get Dividend History to look at how consistent dividends really are and to
# Calculate TTM averages, avergaes per year, etc.
def get_dividend_history(symbol_list):
    
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
                        + " startDate = \"%s-%s-%s\" and endDate = \"%s-%s-%s\""\
                        + " and symbol in (" \
                        +'\'' \
                        + '\',\''.join(remaining_symbols) \
                        + '\'' \
                        + ")"  

        yql = yql % (today.year-10, today.month, today.day, today.year, today.month, today.day)
        result = execute_yql(yql)
        if result != None:            
            remaining_symbols = [symbol for symbol in remaining_symbols if symbol not in result.keys()]
            final = dict(final.items() + result.items())
        else:
            print "done"


    return final




# Take a list of stock ticker symbols and return comprehensive stock data for those symbols
# in a standardized format
def get_combined_data(symbol_list):
    start_time = time.clock()
    
    quotes = get_quotes_data(symbol_list)
    stocks = get_stocks_data(symbol_list)
    key_stats = get_key_stats_data(symbol_list)
    div_hist = get_dividend_history(symbol_list)

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




# Fix output so that there isn't such inconsistency in the data. i.e. "N/A" = 0.00 for a dividend, etc.
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
                
                if value == "N/A" or value == None or value == "None":
                    data[row][item] = 0.00
                elif is_number(value):
                    data[row][item] = float(value)
                else:
                    raise Exception("For "+row+", "+item+": "+value+" is not a valid value.")
                # Does this stock include this field?                    
                

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
                    data[row][item] = float(value.strip('%')) / 100.0





def is_number(s):
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
def is_dividend_stock(stock_data):
    if (type(stock_data) != dict) or 'Symbol' not in stock_data:
        raise Exception("Parameter 'stock_data' must be a dictionary of data for a single stock")

    has_key_stats = not stock_data['NoKeyStats']
    forward_div = stock_data['ForwardAnnualDividendRate']
    has_forward_div = not forward_div == 0.00 or forward_div == "N/A"
    has_div_hist = ('DividendHistory' in stock_data)


    # key stats available
    if has_key_stats:
        if has_div_hist and has_forward_div:
            # This is a dividend stock with history and plan to pay another
            assert stock_data['DividendShare'] > 0.00
            assert stock_data['ForwardAnnualDividendRate'] > 0.00
            assert stock_data['DividendDate'] != None
            assert stock_data['Ex_DividendDate'] != None
            
            return True
        elif not has_forward_div and has_div_hist:
            # This is supposed to be a former dividend stock that cut its dividend
            assert stock_data['DividendShare'] == 0.00
            assert stock_data['ForwardAnnualDividendRate'] == 0.00
            assert stock_data['DividendDate'] == None
            assert stock_data['Ex_DividendDate'] == None

            return False
        elif has_forward_div and not has_div_hist:
            # This is a stock that has no dividends in the past, but is forecasting one
            assert stock_data['DividendShare'] == 0.00
            assert stock_data['ForwardAnnualDividendRate'] > 0.00
            assert stock_data['DividendDate'] != None
            assert stock_data['Ex_DividendDate'] != None

            return True
        else: # not has_forward_div and not has_div_hist: 
            # This is a stock that has no dividend history nor is it forecasting one
            assert float(stock_data['DividendShare']) == 0.00
            assert float(stock_data['ForwardAnnualDividendRate']) == 0.00
            assert stock_data['DividendDate'] == None
            assert stock_data['Ex_DividendDate'] == None

            return False
    
    else: # No key stats available, so only have a dividend share and div history
        if stock_data['DividendShare'] > 0.00 and has_div_hist:
            return True
        elif not (stock_data['DividendShare'] > 0.00 and has_div_hist):
            return False
        else:
            raise Exception("Dividend Share and Div History does not match.")
                    




# A generic way for me to test out tables in real time. Requires that I
# include a symbol column
def get_any_data(symbol_list, table, fields="*"):

    if type(symbol_list) != type( list() ):
        raise Exception("symbol_list must be a list")

    
    yql = "select "+ fields +" from "+table+" where symbol in (" \
                    + '\'' \
                    + '\',\''.join( symbol_list ) \
                    + '\'' \
                    + ")"
                    
    return execute_yql(yql)    













#Unit Test get_quotes_data
def unit_test_get_quotes_data():
    # Test list of quotes with upper and lowercase symbols
    quote_data = get_quotes_data(['aapl', 'T', 'MSFT'])
    
    try:
        price = quote_data['AAPL']['LastTradePriceOnly']
    except:
        return False

    if price <= 0.0:
        return False

    try:
        price = quote_data['T']['LastTradePriceOnly']
    except:
        return False

    if price <= 0.0:
        return False

    try:
        price = quote_data['MSFT']['LastTradePriceOnly']
    except:
        return False

    if price <= 0.0:
        return False

    found = True
    try:
        price = quote_data['WMT']['LastTradePriceOnly']
    except:
        found = False

    if found == True:
        return False

    # Test a list of one, both upper and lowercase
    quote_data = get_quotes_data(['aapl'])
    
    try:
        price = quote_data['AAPL']['LastTradePriceOnly']
    except:
        return False

    quote_data = get_quotes_data(['AAPL'])
    
    try:
        price = quote_data['AAPL']['LastTradePriceOnly']
    except:
        return False

    return True



#Unit Test get_key_stats_data
def unit_test_get_key_stats_data():
    # Test list of quotes with upper and lowercase symbols
    data = get_key_stats_data(['aapl', 'T', 'MSFT'])

    try:
        value = data['AAPL']['TotalDebt']
    except:
        return False

    if value <= 0.0:
        return False

    try:
        value = data['T']['TotalDebt']
    except:
        return False

    if value <= 0.0:
        return False

    try:
        value = data['MSFT']['TotalDebt']
    except:
        return False

    if value <= 0.0:
        return False

    found = True
    try:
        value = data['WMT']['TotalDebt']
    except:
        found = False

    if found == True:
        return False

    # Test a list of one, both upper and lowercase
    data = get_key_stats_data(['aapl'])
    
    try:
        value = data['AAPL']['TotalDebt']
    except:
        return False

    data = get_key_stats_data(['AAPL'])
    
    try:
        value = data['AAPL']['TotalDebt']
    except:
        return False

    return True



#Unit Test get_stocks_data
def unit_test_get_stocks_data():
    # Test list of quotes with upper and lowercase symbols
    data = get_stocks_data(['aapl', 'T', 'MSFT'])
    
    try:
        value = data['AAPL']['start']
    except:
        return False
    
    if value <= 0.0:
        return False
    
    try:
        value = data['T']['start']
    except:
        return False

    if value <= 0.0:
        return False

    try:
        value = data['MSFT']['start']
    except:
        return False

    if value <= 0.0:
        return False

    found = True
    try:
        value = data['WMT']['start']
    except:
        found = False
    
    if found == True:
        return False
    
    # Test a list of one, both upper and lowercase
    data = get_stocks_data(['aapl'])
    
    try:
        value = data['AAPL']['start']
    except:
        return False
    
    data = get_stocks_data(['AAPL'])
    
    try:
        value = data['AAPL']['start']
    except:
        return False
    
    return True


#Unit Test get_stocks_data
def unit_test_get_dividend_history():
    # Test list of quotes with upper and lowercase symbols
    data = get_dividend_history(['aapl', 'T', 'MSFT'])
    
    try:
        value = data['AAPL']['DividendHistory']
    except:
        return False
    
    if value <= 0.0:
        return False
    
    try:
        value = data['T']['DividendHistory']
    except:
        return False

    if value <= 0.0:
        return False

    try:
        value = data['MSFT']['DividendHistory']
    except:
        return False

    if value <= 0.0:
        return False

    found = True
    try:
        value = data['WMT']['DividendHistory']
    except:
        found = False
    
    if found == True:
        return False
    
    # Test a list of one, both upper and lowercase
    data = get_dividend_history(['aapl'])
    
    try:
        value = data['AAPL']['DividendHistory']
    except:
        return False
    
    data = get_dividend_history(['AAPL'])
    
    try:
        value = data['AAPL']['DividendHistory']
    except:
        return False
    
    return True



def test_module():
    pass_tests = True
    pass_tests = pass_tests & unit_test_get_key_stats_data()
    pass_tests = pass_tests & unit_test_get_stocks_data()
    pass_tests = pass_tests & unit_test_get_quotes_data()
    pass_tests = pass_tests & unit_test_get_dividend_history()
    return pass_tests









# Not Needed, I think
# Fix output so that there isn't such inconsistency in the data. i.e. "N/A" = 0.00 for a dividend, etc.
def standardize_data2(data):
    for row in data:
        for item in data[row]:
            #if item is dollar, integer or decimal
            if item in ['LastTradePriceOnly', 'YearLow', 'YearHigh', 'DividendShare', 'EarningsShare',\
                        'PERatio', 'PriceSales', 'PEGRatio', 'ShortRatio', 'BookValue', 'PriceBookTotalDebt', \
                        'ReturnOnEquity', 'TrailingPE', 'RevenuePerShare', 'MarketCap','PriceBook', 'EBITDA', \
                        'OperatingCashFlow', 'Beta', 'ReturnonAssests', 'ForwardAnnualDividendRate', \
                        'SharesShort', 'CurrentRatio', 'BookValuePerShare', 'TotalCashPerShare', 'TotalCash', \
                        'Revenue', 'ForwardPE', 'DilutedEPS', 'SharesOutstanding', 'TotalDebtEquity', \
                        'FullTimeEmployees', 'TotalDebt']:
                value = data[row][item]
                
                if value == "N/A" or value == None or value == "None":
                    data[row][item] = 0
                elif is_number(value):
                    data[row][item] = float(value)
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
                    data[row][item] = None
                else:
                    data[row][item] = float(value.strip('%')) / 100.0
