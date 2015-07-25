__author__ = 'Bruce Nielson'
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
import datetime as dt
from sqlalchemy.orm import relationship, backref, sessionmaker


Base = declarative_base()


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

