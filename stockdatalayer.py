__author__ = 'Bruce Nielson'
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, ForeignKey
import datetime as dt
from sqlalchemy.orm import relationship, backref, sessionmaker
import sqlalchemy.orm.collections as collections
import lxml, lxml.html, requests
import yahoostockdata
import stockdatabase
import logging
import sqlite3
from requests.exceptions import ConnectionError
import inspect
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy import desc
import sqlalchemy
import json
import datetime

Base = declarative_base()


# ORM and Stock Database Classes and Functions


class Datalayer():

    session = None
    database_name = ""
    echo = False
    engine = None
    cef_list = []
    snp500_list = []
    mlp_list = []

    # Initialize database and session
    def __init__(self, database_name="stocks.db", echo=False):
        self.echo = echo
        self.database_name = database_name
        self.engine = create_engine('sqlite:///'+database_name, echo=self.echo)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine,autoflush=False)
        self.session = Session()


    # Reset database and session
    def reset_session(self):
        self.session.close_all()
        self.engine = create_engine('sqlite:///'+self.database_name, echo=self.echo)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()



    # Get stock lists from either database or from web (saved to database)


    def get_cef_list(self, refresh_from_web=False):
        if refresh_from_web == True or self.cef_list == []:
            self.cef_list = self.get_stock_list("CEF", 'http://online.wsj.com/mdc/public/page/2_3024-CEF.html?mod=topnav_2_3040', '//table/tr/td[2]/nobr/a/text()',  \
                              refresh_from_web=refresh_from_web)
        return self.cef_list



    def get_snp500_list(self, refresh_from_web=False):
        if refresh_from_web == True or self.snp500_list == []:
            self.snp500_list = self.get_stock_list("SNP", 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies', '//table[1]/tr/td[1]/a/text()', \
                              refresh_from_web=refresh_from_web)
        if len(self.snp500_list)<500:
            raise Exception("Failed to load whole S&P 500 List from web") #TODO: Replace with a class based error

        return self.snp500_list



    def get_mlp_list(self, refresh_from_web=False):
        if refresh_from_web == True or self.mlp_list == []:
            self.mlp_list = self.get_stock_list("MLP", 'http://www.dividend.com/dividend-stocks/mlp-dividend-stocks.php#', '//div/table[1]/tr/td[1]/a/strong/text()', \
                              refresh_from_web=refresh_from_web)
        return self.mlp_list




    def is_cef(self, symbol):
        if self.cef_list == []:
            self.cef_list = self.get_cef_list()
        return (symbol in self.cef_list)


    def is_mlp(self, symbol):
        if self.mlp_list == []:
            self.mlp_list = self.get_mlp_list()
        return (symbol in self.mlp_list)


    def is_snp500(self, symbol):
        if self.snp500_list == []:
            self.snp500_list = self.get_snp500_list()
        return (symbol in self.snp500_list)



    # list_code can be "SNP", "MLP", or "CEF"
    def get_stock_list(self, list_code, url=None, xpath=None, refresh_from_web=False):
        if refresh_from_web == False:
            result = self.session.query(StockListing).join(Category).filter_by(code=list_code).all()
        else:
            stocklist = self.get_web_table_info(url, xpath)
            if stocklist == []:
                raise Exception("Failed to load stock list from web") #TODO: Replace with a class based error
            cat_id = self.session.query(Category).filter_by(code=list_code).one().id
            self.session.query(StockListing).filter_by(category = cat_id).delete()
            for symbol in stocklist:
                listing = StockListing(symbol=symbol, category=cat_id)
                self.session.add(listing)

            self.session.commit()
            result = self.session.query(StockListing).join(Category).filter_by(code=list_code).all()

        return [row.symbol for row in result]


    # Retrieves a list of all stocks in the database from a list of stocks. If stock_list = None, it retrieves all stocks in the database
    def get_stocks(self, stock_list=None):
        if stock_list == None:
            return self.session.query(Stock).all()
        else:
            return self.session.query(Stock).filter(Stock.symbol.in_(stock_list)).all()


    def get_all_stocks_in_db(self):
        all_stocks = self.get_stocks()
        return [row.symbol for row in all_stocks]



    def get_stocks_by_code(self, list_code):
        return self.get_stocks(self.get_stock_list(list_code.upper()))





    def get_cef_dividend_info(self, cef_symbol):
        if not self.is_cef(cef_symbol):
            raise Exception('Ticker symbol passed was not a CEF')

        #header_list = get_web_stock_list('https://screener.fidelity.com/ftgw/etf/snapshot/distributions.jhtml?symbols='+str(cef_symbol),'//*[contains(@class, "distributinos-capital-gains")]/table/tr[1]/th/text()')
        header_list = ['Ex-Date', 'NAV at Distribution', 'Long-Term Capital Gains', 'Short-Term Capital Gains', 'Dividend Income', 'Return of Capital', 'Distribution Total']
        num_headers = len(header_list)

        # Run through each column
        table_data = self.get_web_table_info('https://screener.fidelity.com/ftgw/etf/snapshot/distributions.jhtml?symbols='+str(cef_symbol),'//*[contains(@class, "distributinos-capital-gains")]/table/tr/td/text()')
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




    @staticmethod
    def get_web_table_info(url, xpath):

        def _get_result(url, xpath):
            # try the new way
            r = requests.get(url)
            root = lxml.html.fromstring(r.content)
            result = root.xpath(xpath)
            return result

        try:
            # Try old way
            # Use libxml to download the list of S&P500 companies and obtain the symbol table
            page = lxml.html.parse(url)
            result = page.xpath(xpath)
        except:
            try:
                result = _get_result(url, xpath)
            except ConnectionError:
                # If I get a ConnectionError, try again and then give up
                try:
                    result = _get_result(url, xpath)
                except ConnectionError:
                    # Failed twice, skip and move on
                    return []
            except:
                raise

        result = [str(item) for item in result]

        return result



    @staticmethod
    def add_new_column(table_name, new_column, column_type, default_val=None, database_name='stocks.db'):
        # Connecting to the database file
        conn = sqlite3.connect(database_name)
        c = conn.cursor()

        if default_val == None:
            c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
                    .format(tn=table_name, cn=new_column, ct=column_type))
        else:
            c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct} DEFAULT '{df}'"\
                    .format(tn=table_name, cn=new_column, ct=column_type, df=default_val))

        # Committing changes and closing the connection to the database file
        conn.commit()
        conn.close()




    # Takes a list of Stocks and gets quote data (real time) for them
    @staticmethod
    def get_real_time_quotes(stock_list):
        symbol_list = [stock.symbol for stock in stock_list if type(stock) == Stock]
        data = yahoostockdata.download_yahoo_quote_data(symbol_list)
        for stock in stock_list:
            if type(stock) == Stock:
                stock.get_quote(data)





    # Table Loading Functions - One time use (Hopefully)


    def init_load_cefs(self, delete_first=False):
        ceflist = self.get_cef_list(refresh_from_web=True)
        return self.load_list(ceflist, "CEF", delete_first=delete_first)



    def init_load_snp500(self, delete_first=False):
        snplist = self.get_snp500_list(refresh_from_web=True)
        return self.load_list(snplist, "SNP", delete_first=delete_first)



    def init_load_mlps(self, delete_first=False):
        mlplist = self.get_mlp_list(refresh_from_web=True)
        return self.load_list(mlplist, "MLP", delete_first=delete_first)





    def init_load_stock_listings(self):
        self.get_snp500_list(refresh_from_web=True)
        self.get_cef_list(refresh_from_web=True)
        self.get_mlp_list(refresh_from_web=True)



    def init_load_categories(self):
        category1 = Category(name="S&P 500", code="SNP")
        category2 = Category(name="Master Limited Partnership", code="MLP")
        category3 = Category(name="Closed End Fund", code="CEF")
        self.session.add(category1)
        self.session.add(category2)
        self.session.add(category3)
        self.session.commit()

        return self.session.query(Category).all()





    def init_load_div_analysis(self):
        stock_list = self.get_stocks()
        for stock in stock_list:
            stock._analyze_dividend_history()
        self.session.commit()
        return stock_list



    def init_load_list(self, stocklist, stock_code, delete_first=False):

        def _combine_dividend_and_distribution_info(div_row, dist_row):
            if type(dist_row) == list:
                if len(dist_row) == 0:
                    return
                if len(dist_row) == 1:
                    matching_dist = dist_row[0]
                else:
                    print "div_row"
                    print div_row
                    print "dist_row"
                    print dist_row
                    raise Exception("Something wrong with the distributions.")
            else:
                matching_dist = dist_row

            div_row.dividend_income = matching_dist['Dividend Income']
            div_row.short_term_cap_gains = matching_dist['Short-Term Capital Gains']
            div_row.long_term_cap_gains = matching_dist['Long-Term Capital Gains']
            div_row.return_of_capital = matching_dist['Return of Capital']
            div_row.nav_at_dist = matching_dist['NAV at Distribution']
            matching_dist['Used'] = True



        if delete_first == True:
            cat_id = self.session.query(Category).filter_by(code=stock_code).one().id
            self.session.query(Stock).filter(Stock.symbol.in_(stocklist)).delete(synchronize_session='fetch')
            self.session.commit()
        else:
            # if we aren't deleting first, then remove from stocklist any that are already in the database
            all_stocks = self.get_all_stocks_in_db()
            stocklist = [symbol for symbol in stocklist if symbol not in all_stocks]

        data = yahoostockdata.get_combined_data(stocklist)

        # Persist data to database
        for symbol in data:
            stock = data[symbol]
            stock_row = Stock(symbol=symbol, company_name=stock['Name'], company_start=stock['start'])

            # Get old data that isn't scrapped any more
            old_data = stockdatabase.retrieve_stock_list_from_db(symbol, 'stocksdataold.db')
            if old_data != []:
                old_data = old_data[0]
                stock_row.industry = old_data[2]
                stock_row.sector = old_data[3]
                stock_row.num_full_time_employees = old_data[5]
            else:
                pass
                # logging.info("No old data for: "+ str(symbol))
                # TODO: Do a screen scrape instead

            if 'DividendHistory' in stock:
                div_hist = stock['DividendHistory']
                cef_distributions = None

                iscef = False
                if self.is_cef(symbol):
                    # Find equivalent distribution row
                    cef_distributions = self.get_cef_dividend_info(symbol)
                    iscef = True

                for div in div_hist:
                    div_row = Dividend()
                    div_row.dividend_date = div['Date']
                    div_row.dividend = div['Dividends']
                    if iscef:
                        # Find equivalent distribution row. Check that date is within 3 days and dividend and distribution match within a penny
                        possible_days = []
                        for i in range(-3, 4, 1):
                            day = div_row.dividend_date + dt.timedelta(days=i)
                            possible_days.append(day)
                        # Try to find an exact match first
                        matching_dist = [dist for dist in cef_distributions if dist['Ex-Date'] == div_row.dividend_date and div_row.dividend - dist['Distribution Total'] <0.01]
                        if len(matching_dist) == 0:
                            # If I didn't find an exact match, find an approximate match
                            matching_dist = [dist for dist in cef_distributions if dist['Ex-Date'] in possible_days and div_row.dividend - dist['Distribution Total'] <0.01]

                        _combine_dividend_and_distribution_info(div_row, matching_dist)

                    stock_row.dividends.append(div_row)

                if cef_distributions != None:
                    not_used = [div for div in cef_distributions if 'Used' not in div or div['Used'] != True]
                    if len(not_used) > 0:
                        # Can we consider this a missing dividend? Check that there is no dividend within 15 days
                        for dist in not_used:
                            possible_days = []
                            dist_date = dist['Ex-Date']
                            for i in range(-15, 16, 1):
                                day =  dist_date + dt.timedelta(days=i)
                                possible_days.append(day)
                            possible_matches = [row for row in stock_row.dividends if div_row.dividend_date in possible_days]

                            # If there are no possible candidates to match at all, then we'll assume this is a missing dividend
                            if len(possible_matches) == 0:
                                div_row = Dividend()
                                div_row.dividend_date = dist['Ex-Date']
                                div_row.dividend = dist['Distribution Total']
                                _combine_dividend_and_distribution_info(div_row, dist)
                                stock_row.dividends.append(div_row)

                    # Did all get used now?
                    not_used = [row for row in not_used if 'Used' not in div or div['Used'] != True]
                    if len(not_used) > 0:
                        print "not used: " + symbol
                        print not_used
                        raise Exception("Some CEF distribution data didn't match dates of dividend data")

            self.session.add(stock_row)

        self.session.commit()

        return self.session.query(Stock).all()






# http://stackoverflow.com/questions/5022066/how-to-serialize-sqlalchemy-result-to-json
# http://solvedstack.com/questions/how-to-serialize-sqlalchemy-result-to-json
# A class used to allow ORM to turn itself to json
class JsonServices():
    def convert_to_jsonifible(self, filter_fields = []):
        _visited_objs = []

        def _do_conversion(obj):

            # Track that we've visisted this item now
            if obj in _visited_objs:
                raise Exception("Visited this object already. " + str(obj.name) +": " + str(obj))

            _visited_objs.append(obj)
            fields = {}

            if isinstance(obj.__class__, DeclarativeMeta) or type(obj) == dict:

                list_of_attr = [attr for attr in dir(obj) if \
                        # Don't process attributes that are hidden, i.e. starts with a "_"
                        (not attr.startswith('_')) and \
                        #Don't include the metadata attribute
                        attr != 'metadata' and \
                        # Don't include the JsonServices methods
                        attr!=obj.convert_to_jsonifible.__name__ and attr!=obj.convert_to_json.__name__ and \
                        #Don't include get_quote because it would call everything recursively
                        attr!='get_quote' and \
                        #Exclude any fields not in the filter_field list, if being used
                        (filter_fields == [] or attr in filter_fields) and \
                        #Don't include any fields we've already visited
                        attr not in _visited_objs]
                for field in list_of_attr:

                    val = obj.__getattribute__(field)


                    # is this field another SQLalchemy object, or a list of SQLalchemy objects?
                    if isinstance(val.__class__, DeclarativeMeta) or type(val) == sqlalchemy.orm.collections.InstrumentedList or type(val) == list:
                        # This is another sqlalchemy object, so recursively evaluate
                        if val in _visited_objs:
                            continue
                        else:
                            fields[field] = _do_conversion(val)

                    elif inspect.ismethod(val):
                        # This is a method, so call it and get the value back
                        fields[field] = val()

                    elif (type(val) == str or
                        type(val) == float or
                        type(val) == unicode or
                        type(val) == int or
                        type(val) == type(None)):
                        fields[field] = val
                    else:
                        fields[field] = str(val)

                # a json-encodable dict
                return fields
            elif type(obj) == sqlalchemy.orm.collections.InstrumentedList or type(obj) == list:
                items = [_do_conversion(item) for item in obj if item not in _visited_objs]
                return items
            else:
                raise Exception(TypeError)

        return _do_conversion(self)



    def convert_to_json(self, filter_fields=[]):
        return json.dumps(self.convert_to_jsonifible(filter_fields))



class Stock(Base, JsonServices):
    __tablename__ = 'stock'
    
    # Company Data - Rarely updates
    id = Column(Integer, primary_key=True)
    symbol = Column(String(5), unique=True, nullable=False)
    company_name = Column(String, default='')
    industry = Column(String, default='')
    sector = Column(String, default='')
    created = Column(DateTime, default=dt.datetime.now())
    company_start = Column(DateTime)
    num_full_time_employees = Column(Integer)
    company_data_last_updated = Column(DateTime, default=dt.datetime.now())

    # Start Key Stats - updates more often
    key_stats_last_updated = Column(DateTime, default=dt.datetime.now())
    # Key Stats - Valuation
    peg_ratio = Column(Float)
    # Key Stats - Financial Highlights - Income
    most_recent_qrt = Column(DateTime)
    qrt_revenue_growth = Column(Float)
    gross_profit = Column(Float)
    ebitda = Column(Float)
    net_income = Column(Float)
    qrt_earnings_growth = Column(Float)
    eps = Column(Float)
    # Key Stats - Balance sheet
    total_cash = Column(Float)
    total_debt = Column(Float)
    current_ratio = Column(Float)
    book_value_per_share = Column(Float)
    # Key Stats - Cash Flow
    operating_cash_flow = Column(Float)
    levered_free_cash = Column(Float)
    # Key Stats - Trading
    beta = Column(Float)
    num_shares = Column(Float)
    shares_short = Column(Float)
    # Key Stats - Dividends
    forward_div = Column(Float)
    trailing_div = Column(Float)
    last_dividend_date = Column(Float)
    last_ex_dividend_date = Column(Float)
    # Dividend analysis attributes
    years_dividends = Column(Float)
    years_div_growth = Column(Float)

    # Relationships to other ORM classes
    dividends = relationship("Dividend", backref='stock',
                    cascade="all, delete, delete-orphan", order_by="desc(Dividend.dividend_date)")
    notes = relationship("Note", backref='stock',
                    cascade="all, delete, delete-orphan", order_by="Note.create_date")
    # Quote fields -- these fields are not in the database and are always updated 'live' via some other services
    last_price = 0.0
    year_low = 0.0
    year_high = 0.0
    trailing_div = 0.0

    # Get a real time stock quote
    def get_quote(self, data=None):
        if data==None:
            data = yahoostockdata.get_quote_data(self.symbol)
        self.last_price = data[self.symbol]['LastTradePriceOnly']
        self.year_low = data[self.symbol]['YearLow']
        self.year_high = data[self.symbol]['YearHigh']
        self.trailing_div =  data[self.symbol]['TrailingAnnualDividendYield']
        self.eps = data[self.symbol]['DilutedEPS']


    # Calculations
    def cash_per_share(self):
        if self.total_cash != None and self.num_shares != None and self.num_shares != 0:
            return float(self.total_cash) / float(self.num_shares)
        else:
            return None


    def debt_per_share(self):
        if self.total_debt != None and self.num_shares != None and self.num_shares != 0:
            return float(self.total_debt) / float(self.num_shares)
        else:
            return None

    def div_yield(self):
        if self.trailing_div != None and self.last_price != None and self.last_price != 0:
            return float(self.trailing_div) / float(self.last_price)
        else:
            return 0.0


    # Dividend history analysis methods and instance variables
    _dividends_no_bonus = None
    _start_of_div_growth_index = None
    _div_growth_start_date = None
    _total_div_growth_amt = None
    _total_div_growth_rate = None
    _div_growth_20 = None
    _div_growth_15 = None
    _div_growth_10 = None
    _div_growth_5 = None
    _div_growth_3 = None
    _div_growth_1 = None

    def _analyze_dividend_history(self):


        def mark_bonus_dividends(div_hist):
            last_index = len(div_hist)-1
            for i in range(0, last_index-1): # We don't compare last element to the one before it because it doens't have one before it
                if not(div_hist[i].dividend >= div_hist[i+1].dividend):
                    # We just found where dividend growth (or stability) potentially ended
                    # But we need to check to see if the trend can continue if we assume this is a "bonus" dividend
                    if (i+2 < last_index) and (div_hist[i].dividend >= div_hist[i+2].dividend):
                        possible_bonus_div = div_hist[i+1]
                    elif (i+1 < last_index) and (div_hist[i-1].dividend >= div_hist[i+1].dividend):
                        possible_bonus_div = div_hist[i]
                    else:
                        continue

                    # The trend continues if we assume this is a "one off" or "bonus" dividend
                    # However, to be considered such, the following criteria must still be met:
                    # 1. This must be a year with 5 or 13 dividends
                    # 2. There must be no other bonus dividend in this year already
                    year = possible_bonus_div.dividend_date.year
                    year_of_divs = [div for div in div_hist if div.dividend_date.year == year]

                    # This can't be a bonus dividend if the # of dividends for a year isn't 5 or 13
                    is_bonus = False
                    if len(year_of_divs) == 5 or len(year_of_divs) == 13:
                        for div in year_of_divs:
                            if div.is_bonus_dividend == False:
                                is_bonus = True
                                break

                    if is_bonus == True:
                        possible_bonus_div.is_bonus_dividend = True




        def years_ago(years, from_date=None):
            if from_date is None:
                from_date = datetime.datetime.now()
            try:
                years = int(years)
                return from_date.replace(year=from_date.year - years)
            except:
                # Must be 2/29!
                assert from_date.month == 2 and from_date.day == 29 # can be removed
                return from_date.replace(month=2, day=28, year=from_date.year-years)


        # Find the dividend amount a number of 'years' ago. Since years is not likely to be exact, find the first dividend
        # on or just before that number of years and return it
        def get_div_growth(div_hist, years):
            # Get most recent div
            most_recent_div = div_hist[0]
            most_recent_date = most_recent_div.dividend_date
            # Search for dividend years ago (minus 5 days to try to capture slight differences in date for ex-dividend)
            target_date = years_ago(years, most_recent_date) - datetime.timedelta(days=5)
            for div in div_hist:
                div_date = div.dividend_date
                if div_date <= target_date:
                    div_growth_amt = float(most_recent_div.dividend) - float(div.dividend)
                    div_growth_rate = div_growth_amt / float(div.dividend)
                    return div_growth_rate

            # We ran out of dividends, so just take the very first one:
            div_hist_len = len(div_hist)
            div = div_hist[div_hist_len-1]
            div_growth_amt = float(most_recent_div.dividend) - float(div.dividend)
            div_growth_rate = div_growth_amt / float(div.dividend)
            return div_growth_rate





        def create_div_totals_by_year(div_hist):
            current_year = datetime.datetime.now().year
            last_index = len(div_hist)-1
            start_div_date = div_hist[last_index].dividend_date
            start_year =  start_div_date.year

            divs_per_year = []
            if start_year < current_year:
                for year in list(reversed(range(start_year, current_year+1))):
                    tot_per_year = sum([div.dividend for div in div_hist if div.dividend_date.year == year])
                    divs_per_year.append((year, tot_per_year))

            return divs_per_year




        def find_start_of_div_growth(div_hist, tot_divs_per_year=[], start=0):
            # Find last_index based total dividends for year: i.e. dividends must be non-zero and equal to or less (i.e. growth) then the previous year

            # If the user didn't pass the divs per year, we create them
            if len(tot_divs_per_year) == 0:
                tot_divs_per_year = create_div_totals_by_year(div_hist)

            # Find last year of yearly dividend growth or last non-zero year
            last = len(tot_divs_per_year)-1
            last_index = len(div_hist)-1
            if last >= 0:
                start_year = tot_divs_per_year[last][0]
                end_year = tot_divs_per_year[0][0]
            else:
                start_div_date = div_hist[last_index].dividend_date
                start_year =  start_div_date.year
                end_year = start_year

            last_year_of_growth = 0
            if start_year != end_year:
                years = list(reversed(range(start_year, end_year+1)))
                for i in range(0, len(years)):
                    # Continue searching while we're dealing with a non-zero year
                    if tot_divs_per_year[i][1] > 0.0:
                        # Check if we're on final year yet
                        if i < len(years)-1:
                            div_this_year = tot_divs_per_year[i][1]
                            div_next_year = tot_divs_per_year[i+1][1]
                            if not(div_this_year >= div_next_year):
                                # next years dividend ends growth phase
                                last_year_of_growth = tot_divs_per_year[i][0]
                                break
                        else: # Else i = final year, so we're already on last year
                            last_year_of_growth = tot_divs_per_year[i][0]
                            break
                    else: # Else we found a zero dividend year
                        # This year has no dividends, so previous year is last year
                        last_year_of_growth = tot_divs_per_year[i-1][0]
                        break
            else:
                last_year_of_growth = start_year

            # We found last year of growth - now translate that to last index to search for growth in: Not sure if I should search one year beyond or not. You get problems either way
            div_hist = [div for div in div_hist if div.dividend_date.year >= last_year_of_growth]

            last_index = len(div_hist)-1

            # Now find last dividend of growth within restricted range
            if start > last_index:
                symbol = div_hist[0].symbol
                raise Exception("Start ("+str(start)+") must not be less than length remaining ("+str(last_index)+") to search for stock: " + str(symbol))
            for i in range(start, last_index): #We don't compare last element to the one before it because it doens't have one before it
                if not(div_hist[i].dividend >= div_hist[i+1].dividend):
                    # We just found where dividend growth (or stability) ended
                    div_hist[i]._is_start_of_growth = True
                    return i


            # if we never find a point where growth begins, then the very first dividend is that starting point
            div_hist[last_index]._is_start_of_growth = True
            return last_index





        # Determine how long the stock has paid dividends for
        if len(self.dividends) < 1:
            return
        div_hist = self.dividends
        most_recent_div_date = div_hist[0].dividend_date
        last_index = len(div_hist)-1
        # If this stock has only one dividend, then abort
        if last_index == 0:
            return

        start_div_date = div_hist[last_index].dividend_date
        div_hist_len = ((most_recent_div_date - start_div_date).days / 365.25)
        self.years_dividends = div_hist_len

        # Determine Dividend Growth
        # "dividend growth" is misleading here. We're really looking for growth or stability.
        # So what this function really does is look for a dividend in the past that is larger than the ones that come after it
        mark_bonus_dividends(div_hist)
        # make a list of dividends without bonuses
        self._dividends_no_bonus = [div for div in div_hist if div.is_bonus_dividend == False]

        # What's the real dividend per year? (Not including current year because its only partially complete and thus always too low)
        tot_divs_per_year_no_bonus = create_div_totals_by_year(self._dividends_no_bonus)[1:]
        tot_divs_per_year = create_div_totals_by_year(div_hist)[1:]

        # TODO Calculate a trailing year of dividends not including bonus dividends

        self._start_of_div_growth_index = find_start_of_div_growth(self._dividends_no_bonus, tot_divs_per_year_no_bonus)
        div_growth_start_div = self.dividends[self._start_of_div_growth_index]
        self._div_growth_start_date = div_growth_start_div.dividend_date
        self.years_div_growth = ((most_recent_div_date - self._div_growth_start_date).days / 365.25)

        # Determine Total Growth
        if self.years_div_growth >= 1.0:
            most_recent_div = div_hist[0]

            self._total_div_growth_amt = float(most_recent_div.dividend) - float(div_growth_start_div.dividend)
            self._total_div_growth_rate = self._total_div_growth_amt / float(div_growth_start_div.dividend)
            # Growth amounts by year
            if self.years_div_growth >= 20.0:
                self._div_growth_20 = get_div_growth(div_hist, 20) / 20.0
            if self.years_div_growth >= 15.0:
                self._div_growth_15  = get_div_growth(div_hist, 15) / 15.0
            if self.years_div_growth >= 10.0:
                self._div_growth_10  = get_div_growth(div_hist, 10) / 10.0
            if self.years_div_growth >= 5.0:
                self._div_growth_5  = get_div_growth(div_hist, 5) / 5.0
            if self.years_div_growth >= 3.0:
                self._div_growth_3 = get_div_growth(div_hist, 3) / 3.0
            if self.years_div_growth >= 1.0:
                self._div_growth_1  = get_div_growth(div_hist, 1)


    """
    # Public interface for dividend attributes
    def years_dividends(self):
        if self._years_of_dividends == None:
            self._analyze_dividend_history()
        return self._years_of_dividends

    def years_div_growth(self):
        if len(self.dividends) > 1:
            if self._div_growth_start_date == None:
                self._analyze_dividend_history()
            start_div_date = self._div_growth_start_date
            most_recent_div_date = self.dividends[0].dividend_date
            return ((most_recent_div_date - start_div_date).days / 365.25)
        else:
            return None
    """


    def __repr__(self):
        return "<Stock(symbol='%s', company_name='%s', id='%s')>" % \
               (self.symbol, self.company_name, self.id)


class Dividend(Base, JsonServices):
    __tablename__ = 'dividend_history'
        
    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stock.id'))
    dividend_date = Column(DateTime, nullable=False)
    dividend = Column(Float, nullable=False)
    # For CEF distributions
    nav_at_dist = Column(Float, nullable=True)
    long_term_cap_gains = Column(Float, nullable=True)
    short_term_cap_gains = Column(Float, nullable=True)
    dividend_income = Column(Float, nullable=True)
    return_of_capital = Column(Float, nullable=True)

    # Dividend analysis attributes
    is_bonus_dividend = False
    _is_start_of_growth = False

    def __repr__(self):
        return "<Stock(symbol='%s', dividend_date='%s', dividend='%s', id='%s')>" % \
               (self.stock.symbol, self.dividend_date, self.dividend, self.id)


class Note(Base, JsonServices):
    __tablename__ = 'stock_note'

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stock.id'))
    create_date = Column(DateTime)
    note = Column(String)
    url = Column(String)

    def __repr__(self):
        return "<Stock(symbol='%s', note='%s', url='%s', id='%s')>" % \
               (self.stock.symbol, self.note, self.url, self.id)
    


class StockListing(Base, JsonServices):
    __tablename__ = 'stock_listing'
    id = Column(Integer, primary_key=True)
    symbol = Column(String(5), nullable=False) #unique=True
    category = Column(Integer, ForeignKey('category.id'))

    def __repr__(self):
        return "<StockListing(symbol='%s', category='%s', id='%s')>" % (self.symbol, self.category, self.id)

class Category(Base, JsonServices):
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    code = Column(String(3), unique=True, nullable=False)

    def __repr__(self):
        return "<Category(name='%s', code='%s', id='%s')>" % (self.name, self.code, self.id)






def test_encoder():
    datalayer = Datalayer()
    stocks = datalayer.get_stocks(['T', 'GOOG'])
    #decoded = convert_to_dict(stocks, )
    for stock in stocks:
        decoded = stock.convert_to_jsonifible()
        print decoded


