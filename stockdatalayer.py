__author__ = 'Bruce Nielson'
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
import datetime as dt
from sqlalchemy.orm import relationship, backref, sessionmaker
import lxml, lxml.html, requests
import yahoostockdata
import stockdatabase
import logging


Base = declarative_base()

# ORM and Stock Database Classes and Functions


def initialize_datalayer(database_name="stocks.db"):
    engine = create_engine('sqlite:///'+database_name)#, echo=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


    
class Stock(Base):
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

    dividends = relationship("Dividend", backref='stock',
                    cascade="all, delete, delete-orphan")
    notes = relationship("Note", backref='stock',
                    cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<Stock(symbol='%s', company_name='%s', id='%s')>" % \
               (self.symbol, self.company_name, self.id)
    



class Dividend(Base):
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

    def __repr__(self):
        return "<Stock(symbol='%s', dividend_date='%s', dividend='%s', id='%s')>" % \
               (self.stock.symbol, self.dividend_date, self.dividend, self.id)




class Note(Base):
    __tablename__ = 'stock_note'

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stock.id'))
    note = Column(String)
    url = Column(String)

    def __repr__(self):
        return "<Stock(symbol='%s', note='%s', url='%s', id='%s')>" % \
               (self.stock.symbol, self.note, self.url, self.id)
    


def test_database():
    session = initialize_datalayer()
    stock1 = Stock()
    dividend1 = Dividend()
    dividend2 = Dividend()

    try:
        stock3 = session.query(Stock).filter_by(symbol='TC').one()

        if type(stock3) == Stock:
            session.delete(stock3)
            session.commit()
    except:
        pass

    stock1.company_name = "The Company"
    stock1.symbol = 'TC'
    stock1.eps = 1.5
    stock1.forward_div = 2.6


    dividend1.dividend = .45
    dividend1.dividend_date = dt.datetime.strptime('4/5/2015',"%m/%d/%Y")
    stock1.dividends.append(dividend1)
    dividend2.dividend = .44
    dividend2.dividend_date = dt.datetime.strptime('4/5/2014',"%m/%d/%Y")
    stock1.dividends.append(dividend2)

    session.add(stock1)
    session.commit()

    stock2 = session.query(Stock).filter_by(symbol='TC').one()
    return stock2




class StockListing(Base):
    __tablename__ = 'stock_listing'
    id = Column(Integer, primary_key=True)
    symbol = Column(String(5), nullable=False) #unique=True
    category = Column(Integer, ForeignKey('category.id'))

    def __repr__(self):
        return "<StockListing(symbol='%s', category='%s', id='%s')>" % (self.symbol, self.category, self.id)

class Category(Base):
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    code = Column(String(3), unique=True, nullable=False)

    def __repr__(self):
        return "<Category(name='%s', code='%s', id='%s')>" % (self.name, self.code, self.id)





# Web Scrapping Data Sources


def get_snp500_list(refresh_from_web=False, session=None, database_name="stocks.db"):
    snplist = get_stock_list("SNP", 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies', '//table[1]/tr/td[1]/a/text()', \
                          refresh_from_web=refresh_from_web, session=session, database_name=database_name)
    if len(snplist)<500:
        raise Exception("Failed to load whole S&P 500 List from web") #TODO: Replace with a class based error

    return snplist



def get_cef_list(refresh_from_web=False, session=None, database_name="stocks.db"):
    ceflist = get_stock_list("CEF", 'http://online.wsj.com/mdc/public/page/2_3024-CEF.html?mod=topnav_2_3040', '//table/tr/td[2]/nobr/a/text()',  \
                          refresh_from_web=refresh_from_web, session=session, database_name=database_name)
    return ceflist


def get_mlp_list(refresh_from_web=False, session=None, database_name="stocks.db"):
    mlplist = get_stock_list("MLP", 'http://www.dividend.com/dividend-stocks/mlp-dividend-stocks.php#', '//div/table[1]/tr/td[1]/a/strong/text()', \
                          refresh_from_web=refresh_from_web, session=session, database_name=database_name)
    return mlplist



# Get a list of all current S&P 500 stocks off of Wikipedia.
# Since Wikipedia is constantly updated, this should always be an
# up to date list.
def get_stock_list(list_code, url, xpath, refresh_from_web=False, session=None, database_name="stocks.db"):
    if session == None:
        session = initialize_datalayer(database_name)

    if refresh_from_web == False:
        result = session.query(StockListing).join(Category).filter_by(code=list_code).all()
    else:
        stocklist = get_web_table_info(url, xpath)
        if stocklist == []:
            raise Exception("Failed to load stock list from web") #TODO: Replace with a class based error
        cat_id = session.query(Category).filter_by(code=list_code).one().id
        session.query(StockListing).filter_by(category = cat_id).delete()
        for symbol in stocklist:
            listing = StockListing(symbol=symbol, category=cat_id)
            session.add(listing)

        session.commit()
        result = session.query(StockListing).join(Category).filter_by(code=list_code).all()

    return result





def get_web_table_info(url, xpath):
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
            raise

    result = [str(item) for item in result]

    return result





# Table Loading Functions - One time use (Hopefully)

def load_snp500(database_name="stocks.db", delete_first=False):
    session = initialize_datalayer(database_name)
    snplist = [listing.symbol for listing in get_snp500_list()]

    if delete_first == True:
        cat_id = session.query(Category).filter_by(code="SNP").one().id
        session.query(Stock).filter(Stock.symbol.in_(snplist)).delete(synchronize_session='fetch')
        session.commit()

    snp = yahoostockdata.get_combined_data(snplist)

    # Persist data to database
    for symbol in snp:
        stock = snp[symbol]
        stock_row = Stock(symbol=symbol, company_name=stock['Name'], company_start=stock['start'])

        # Get old data that isn't scrapped any more
        old_data = stockdatabase.retrieve_stock_list_from_db(symbol, 'stocksdataold.db')
        if old_data != []:
            old_data = old_data[0]
            stock_row.industry = old_data[2]
            stock_row.sector = old_data[3]
            stock_row.num_full_time_employees = old_data[5]
        else:
            logging.info("No old data for: "+ str(symbol))

        if 'DividendHistory' in stock:
            div_hist = stock['DividendHistory']

            for div in div_hist:
                div_row = Dividend()
                div_row.dividend_date = div['Date']
                div_row.dividend = div['Dividends']
                stock_row.dividends.append(div_row)

        session.add(stock_row)

    session.commit()

    return session.query(Stock).all()





def load_categories(database_name="stocks.db"):
    session = initialize_datalayer(database_name)
    category1 = Category(name="S&P 500", code="SNP")
    category2 = Category(name="Master Limited Partnership", code="MLP")
    category3 = Category(name="Closed End Fund", code="CEF")
    session.add(category1)
    session.add(category2)
    session.add(category3)
    session.commit()

    return session.query(Category).all()


def load_stock_listings(database_name="stocks.db"):
    get_snp500_list(refresh_from_web=True)
    get_cef_list(refresh_from_web=True)
    get_mlp_list(refresh_from_web=True)



