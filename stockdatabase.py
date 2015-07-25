import lxml, lxml.html, requests
import sqlite3
import os
import pickle
import yahoostockdata
import datetime
import logging


__author__ = 'Bruce Nielson'


logging.basicConfig(filename="stocks.log",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)


QUOTE_FIELDS = "symbol, Name, YearLow, YearHigh,  DaysLow, DaysHigh, OneyrTargetPrice, EarningsShare,  EPSEstimateCurrentYear, EPSEstimateNextYear, "\
    + "EPSEstimateNextQuarter, BookValue, DividendPayDate, ExDividendDate, DividendShare"






# Create the SQLite database to store stock information for all stocks we're
# interested in. Stock data that doesn't need to get updated all the time is
# stored here. There is an optional parameter to specify a database name.
# This is only used for unit testing so that I don't have to destroy the
# existing database each time the test runs.
def create_database(database_name = "stocksdata.db"):
    # Create or open the database
    db = sqlite3.connect(os.path.dirname(__file__)+"\\"+database_name, detect_types=sqlite3.PARSE_DECLTYPES)

    # if database already exists, drop all indexes then tables
    db.execute('drop index if exists symbolx')
    db.execute('drop index if exists div_datex')
    db.execute('drop index if exists div_stockx')
    db.execute('drop index if exists div_stock_datex')
    db.execute('drop index if exists stats_stockx')

    db.execute('drop table if exists stock_info')
    db.execute('drop table if exists dividend_history')
    db.execute('drop table if exists key_stats')
    db.execute('drop table if exists note')


    # create the tables
    db.execute('create table stock_info(id INTEGER PRIMARY KEY, symbol TEXT, company_name TEXT, industry TEXT,'\
                        + 'sector TEXT, start TEXT, full_time_employees INTEGER, last_updated TEXT)')
    db.execute('create table dividend_history (id INTEGER PRIMARY KEY, stockid INTEGER, dividends REAL, date TEXT)')
    db.execute('create table key_stats (id INTEGER PRIMARY KEY, stockid INTEGER, '\
                        # Valuation
                        + 'peg_ratio REAL, '\
                        # Financial highlights: income
                        + 'recent_quarter TEXT, revenue_ttm REAL, qrt_revenue_growth REAL, gross_profit REAL, ebitda REAL, net_income REAL, qrt_earnings_growth REAL, eps REAL,'\
                        # balance sheet
                        + 'total_cash REAL, total_debut REAL, current_ratio REAL, book_value_share REAL'\
                        # cash flow
                        + 'operating_cash_flow REAL, levered_free_cash REAL,'\
                        # trading
                        + 'beta REAL, num_shares INTEGER, shares_short INTEGER,'\
                        # dividends
                        + 'forward_div REAL, trailing_div REAL, dividend_date TEXT, ex_dividend_date TEXT, last_updated TEXT'
                        + ')')
    db.execute('create table note(id INTEGER PRIMARY KEY, stockid INTEGER, note TEXT, url TEXT)')

    # Create the indexes on the tables
    db.execute('create unique index if not exists symbolx on stock_info(symbol)')
    db.execute('create index if not exists div_datex on dividend_history(date)')
    db.execute('create index if not exists div_stockx on dividend_history(stockid)')
    db.execute('create unique index if not exists div_stock_datex on dividend_history(stockid, date)')
    db.execute('create unique index if not exists stats_stockx on key_stats(stockid)')
    db.execute('create unique index if not exists note_stockx on note(stockid)')

    db.commit()
    db.close()




def execute_quere(sql, database = "stocksdata.db"):
    try:
        db = sqlite3.connect(os.path.dirname(__file__)+"\\"+database)
        cursor = db.cursor()
        cursor.execute(sql)
    except Exception, e:
        logging.error(e)
    finally:
        result = cursor.fetchall()
        db.close()

    return result



def insert_stock_data(stocks_data, database = "stocksdata.db"):
    insert_stocks_list = []
    db = sqlite3.connect(os.path.dirname(__file__)+"\\"+database)
    cursor = db.cursor()
    for symbol in stocks_data.keys():
        #symbol TEXT, company_name TEXT, industry TEXT, sector TEXT, start TEXT, full_time_employees INTEGER, has_dividends INTEGER, last_dividend_date TEXT, last_updated TEXT
        # get stocks data
        company_name = stocks_data[symbol]['Name']
        if company_name == "":
            company_name = stocks_data[symbol]['CompanyName']
        industry = stocks_data[symbol]["Industry"]
        sector = stocks_data[symbol]["Sector"]
        start = stocks_data[symbol]["start"]
        full_time_employees = stocks_data[symbol]["FullTimeEmployees"]
        last_updated = datetime.datetime.now()

        if 'DividendHistory' in stocks_data[symbol]:
            div_hist_data = stocks_data[symbol]["DividendHistory"]
            if len(div_hist_data) > 0:
                has_dividends = True
            else:
                has_dividends = False
            try:
                last_dividend_date = div_hist_data[symbol]["DividendHistory"][0]['Date']
            except:
                last_dividend_date = None

        stocks_data_row = (symbol, industry, sector, start, full_time_employees, has_dividends, last_dividend_date, last_updated)
        insert_stocks_list.append(stocks_data_row)

    # Do insert for new symbols that we want to store
    db.executemany("insert into stock_list(symbol, industry, sector, start, full_time_employees, has_dividends, last_dividend_date, last_updated) "\
            + "values (?, ?, ?, ?, ?, ?, ?, ?)", insert_stocks_list)
    db.commit()







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
    db = sqlite3.connect(os.path.dirname(__file__)+"\\"+database)
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
    stocks_data = yahoostockdata.get_stock_data(new_symbols)
    data = yahoostockdata.get_dividend_history_data(new_symbols)
    div_hist_data = data

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




# Retrieve the entire stock_list table from the database with all fields.
# Returns a list with a tuple for all rows with tuple in this format:
# (rowid, symbol, industry, sector, start, full_time_employees, has_dividends,
# last_dividend_date, last_updated).
# If the optional parameter is set to "", retrieve entire table, otherwise,
# retrieve just one symbol. There is also an optional parameter to specify a
# database name. This is only used for unit testing so that I don't have to
# destroy the existing database each time the test is ran.
def retrieve_stock_list_from_db(symbol = "", database="stocksdata.db"):
    db = sqlite3.connect(os.path.dirname(__file__)+"\\"+database)
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

    db = sqlite3.connect(os.path.dirname(__file__)+"\\"+database)
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
    db = sqlite3.connect(os.path.dirname(__file__)+"\\stocksdata.db")
    db_stock_data = db.execute('select symbol, industry,'\
                        + 'sector, start, full_time_employees, has_dividends, '\
                        + 'last_dividend_date, last_updated from stock_list')

    # Loop through each entry in the stock_data dictionary
    # and compare it to the existing data to determine what should be saved.

    # If it is not already in the database, do an insert.
    # If it is, then do an update.
    stored_symbols = [row[yahoostockdata.SYMBOL] for row in db_stock_data]
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







def get_cef_dividend_info(cef_symbol):
    if cef_symbol not in get_cef_list(True):
        raise Exception('Ticker symbol passed was not a CEF')

    #header_list = get_web_stock_list('https://screener.fidelity.com/ftgw/etf/snapshot/distributions.jhtml?symbols='+str(cef_symbol),'//*[contains(@class, "distributinos-capital-gains")]/table/tr[1]/th/text()')
    header_list = ['Ex-Date', 'NAV at Distribution', 'Long-Term Capital Gains', 'Short-Term Capital Gains', 'Dividend Income', 'Return of Capital', 'Distribution Total']
    num_headers = len(header_list)

    # Run through each column
    table_data = get_web_table_info('https://screener.fidelity.com/ftgw/etf/snapshot/distributions.jhtml?symbols='+str(cef_symbol),'//*[contains(@class, "distributinos-capital-gains")]/table/tr/td/text()')
    data_size = len(table_data)

    if data_size % num_headers != 0:
        raise Exception('Data in table does not match expected number of columns')

    i = 0
    row = 0
    result = []
    while i < data_size:
        # fill in one row
        result.append({})
        for j in range(0,num_headers):
            header_label = header_list[j]
            value = table_data[i+j]
            if header_label == 'Ex-Date':
                value = yahoostockdata.convert_to_date(value)
            else:
                value = yahoostockdata.convert_to_float(value)

            result[row][header_list[j]] = value
        result[row]['Symbol'] = cef_symbol
        row += 1
        i += num_headers

    return result




import BeautifulSoup
def get_tables(htmldoc):
    soup = BeautifulSoup.BeautifulSoup(htmldoc)
    return soup




def get_mlp_list(get_local=False):
    if get_local == True:
        return get_pickled_list('mlplist')
    else:
        return get_web_table_info('http://www.dividend.com/dividend-stocks/mlp-dividend-stocks.php#', '//div/table[1]/tr/td[1]/a/strong/text()', 'mlplist')



def get_cef_list(get_local=False):
    if get_local == True:
        return get_pickled_list('ceflist')
    else:
        return get_web_table_info('http://online.wsj.com/mdc/public/page/2_3024-CEF.html?mod=topnav_2_3040', '//table/tr/td[2]/nobr/a/text()', 'ceflist')



# Get a list of all current S&P 500 stocks off of Wikipedia.
# Since Wikipedia is constantly updated, this should always be an
# up to date list.
def get_snp500_list(get_local=False):
    if get_local == True:
        return get_pickled_list('snplist')
    else:
        return get_web_table_info('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies', '//table[1]/tr/td[1]/a/text()', 'snplist')




def get_pickled_list(list_name):
    f = open(os.path.dirname(__file__)+"\\" + list_name + '.txt')
    sl = pickle.load(f)
    f.close()
    return sl


def pickle_list(list_data, list_name):
    file_name = os.path.dirname(__file__)+"\\" + str(list_name) + '.txt'
    try:
        os.remove(file_name)
    except:
        pass

    f = open(os.path.dirname(__file__)+"\\" + str(list_name) + '.txt', 'w')
    pickle.dump(list_data, f)
    f.close()



def get_web_table_info(url, xpath, file_name=None):
    # Stores the current time, for the created_at record
    now = datetime.datetime.utcnow()

    # TODO: Can I rewrite this to work without lxml package which isn't standard?
    try:
        # Try old way
        # Use libxml to download the list of S&P500 companies and obtain the symbol table
        page = lxml.html.parse(url)
        result = page.xpath(xpath)
    except:
        try:
            # try the new way
            r = requests.get(url)
            root = lxml.html.fromstring(r.content)
            result = root.xpath(xpath)
        except:
            # both ways failed, so load from pickle
            if file_name != None:
                f = open(os.path.dirname(__file__)+"\\" + str(file_name) + '.txt')
                symbol_list = []
                symbol_list = pickle.load(f)
                f.close()
            else:
                raise Exception('Failed to load list.')

    result = [str(item) for item in result]
    # Pickle result for future retrieval if page can't be reached
    if file_name != None:
        pickle_list(result, file_name)

    return result





def sample_code1():
    # https://blog.scraperwiki.com/2011/12/how-to-scrape-and-parse-wikipedia/
    import lxml.etree
    import urllib

    title = "Aquamole Pot"

    params = { "format":"xml", "action":"query", "prop":"revisions", "rvprop":"timestamp|user|comment|content" }
    params["titles"] = "API|%s" % urllib.quote(title.encode("utf8"))
    qs = "&".join("%s=%s" % (k, v)  for k, v in params.items())
    url = "http://en.wikipedia.org/w/api.php?%s" % qs
    tree = lxml.etree.parse(urllib.urlopen(url))
    revs = tree.xpath('//rev')

    print "The Wikipedia text for", title, "is"
    print revs[-1].text






def pickle_stock_data(data):
    file_name = os.path.dirname(__file__)+"\\" + 'stockdata.txt'
    try:
        os.remove(file_name)
    except:
        pass

    f = open(file_name, 'w')
    pickle.dump(data, f)
    f.close()



def get_pickle_stock_data():
    file_name = os.path.dirname(__file__)+"\\" + 'stockdata.txt'
    f = open(file_name)
    sl = pickle.load(f)
    f.close()
    return sl





