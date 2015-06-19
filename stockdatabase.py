import lxml, lxml.html, requests
import sqlite3
import os
import pickle
from yahoostockdata import *
import datetime

__author__ = 'Bruce Nielson'



# Create the SQLite database to store stock information for all stocks we're
# interested in. Stock data that doesn't need to get updated all the time is
# stored here. There is an optional parameter to specify a database name.
# This is only used for unit testing so that I don't have to destroy the
# existing database each time the test runs.
def create_database(database_name = "stocksdata.db"):
    # Create or open the database
    db = sqlite3.connect(os.path.dirname(__file__)+"\\"+database_name)

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




def get_mlp_list():
    return get_web_stock_list('http://www.dividend.com/dividend-stocks/mlp-dividend-stocks.php#', '//div/table[1]/tr/td[1]/a/strong/text()')



def get_cef_list():
    return get_web_stock_list('http://online.wsj.com/mdc/public/page/2_3024-CEF.html?mod=topnav_2_3040', '//table/tr/td[2]/nobr/a/text()')



# Get a list of all current S&P 500 stocks off of Wikipedia.
# Since Wikipedia is constantly updated, this should always be an
# up to date list.
def get_wikipedia_snp500_list():
    return get_web_stock_list('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies', '//table[1]/tr/td[1]/a/text()')



def get_web_stock_list(url, xpath):
    # Stores the current time, for the created_at record
    now = datetime.datetime.utcnow()

    # TODO: Can I rewrite this to work without lxml package which isn't standard?
    try:
        # Try old way
        # Use libxml to download the list of S&P500 companies and obtain the symbol table
        page = lxml.html.parse(url)
        symbol_list = page.xpath(xpath)
    except:
        r = requests.get(url)
        root = lxml.html.fromstring(r.content)
        symbol_list = root.xpath(xpath)

    return [str(item) for item in symbol_list]







def test():
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


def pickle_snp_500_list():
    symbol_list = get_wikipedia_snp500_list()
    f = open(os.path.dirname(__file__)+"\\"+'snplist.txt', 'w')
    pickle.dump(symbol_list, f)
    f.close()




def get_pickled_snp_500_list():
    f = open(os.path.dirname(__file__)+"\\"+'snplist.txt')
    sl = pickle.load(f)
    f.close()
    return sl




def pickle_stock_data(data):
    f = open(os.path.dirname(__file__)+"\\"+'snpdata.txt', 'w')
    pickle.dump(data, f)
    f.close()



def get_pickle_stock_data():
    f = open(os.path.dirname(__file__)+"\\"+'snpdata.txt')
    sl = pickle.load(f)
    f.close()
    return sl






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
    stocks_data = get_stock_data(new_symbols)
    data = get_dividend_history_data(new_symbols)
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

