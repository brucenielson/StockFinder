__author__ = 'bruce'
import datetime


def sqlalchemy_tutorial():
    # From: http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html

    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///:memory:')#, echo=True)
    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()
    from sqlalchemy import Column, Integer, String

    class User(Base):
        __tablename__ = 'users'

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)
        password = Column(String)

        def __repr__(self):
            return "<User(name='%s', fullname='%s', password='%s')>" % \
                   (self.name, self.fullname, self.password)

    Base.metadata.create_all(engine)

    ed_user = User(name='ed', fullname='Ed Jones', password='edspassword')

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()
    session.add(ed_user)
    our_user = session.query(User).filter_by(name='ed').first()
    print our_user
    print ed_user is our_user

    session.add_all([
        User(name='wendy', fullname='Wendy Williams', password='foobar'),
        User(name='mary', fullname='Mary Contrary', password='xxg527'),
        User(name='fred', fullname='Fred Flinstone', password='blah')])

    ed_user.password = 'f8s7ccs'
    print session.dirty
    print session.new

    session.commit()


    for instance in session.query(User).order_by(User.id):
        print instance.name, instance.fullname

    for name, fullname in session.query(User.name, User.fullname):
        print name, fullname

    for row in session.query(User, User.name).all():
        print row.User, row.name

    for u in session.query(User).order_by(User.id)[1:3]:
        print u

    for name, in session.query(User.name).\
        filter_by(fullname='Ed Jones'):
        print name

    for user in session.query(User).\
        filter(User.name=='ed').\
        filter(User.fullname=='Ed Jones'):
        print "___"
        print user



    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import relationship, backref

    class Address(Base):
        __tablename__ = 'addresses'
        id = Column(Integer, primary_key=True)
        email_address = Column(String, nullable=False)
        user_id = Column(Integer, ForeignKey('users.id'))

        user = relationship("User", backref=backref('addresses', order_by=id))
        def __repr__(self):
            return "<Address(email_address='%s')>" % self.email_address

    Base.metadata.create_all(engine)
    jack = User(name='jack', fullname='Jack Bean', password='gjffdd')
    print jack
    print jack.addresses

    jack.addresses = [
        Address(email_address='jack@google.com'),
        Address(email_address='j25@yahoo.com')]

    print jack.addresses[1]
    print jack.addresses[1].user

    session.add(jack)
    session.commit()

    jack = session.query(User).filter_by(name='jack').one()
    print jack
    print jack.addresses

    for u, a in session.query(User, Address).\
        filter(User.id==Address.user_id).\
        filter(Address.email_address=='jack@google.com').\
        all():
        print u
        print a

    print "__________"
    print session.query(User).join(Address).\
        filter(Address.email_address=='jack@google.com').\
        all()


    """SELECT users.*, adr_count.address_count FROM users LEFT OUTER JOIN
        (SELECT user_id, count(*) AS address_count
            FROM addresses GROUP BY user_id) AS adr_count
        ON users.id=adr_count.user_id
    """
    from sqlalchemy.sql import func
    stmt = session.query(Address.user_id, func.count('*').\
        label('address_count')).\
        group_by(Address.user_id).subquery()

    print stmt

    for u, count in session.query(User, stmt.c.address_count).\
        outerjoin(stmt, User.id==stmt.c.user_id).order_by(User.id):
        print u, count


    for name, in session.query(User.name).\
        filter(User.addresses.any()):
        print name


    for name, in session.query(User.name).\
        filter(User.addresses.any(Address.email_address.like('%google%'))):
        print name


    print session.query(Address).\
        filter(~Address.user.has(User.name=='jack')).all()



    class User2(Base):
        __tablename__ = 'users2'

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)
        password = Column(String)

        addresses = relationship("Address2", backref='userz',
                        cascade="all, delete, delete-orphan")

        def __repr__(self):
            return "<User(name='%s', fullname='%s', password'%s')>" % (
                self.name, self.fullname, self.password)


    class Address2(Base):
        __tablename__ = 'addresses2'
        id = Column(Integer, primary_key=True)
        email_address = Column(String, nullable=False)
        user_id = Column(Integer, ForeignKey('users2.id'))

        def __repr__(self):
            return "<Address(email_address='%s')>" % self.email_address


    Base.metadata.create_all(engine)
    jack2 = User2(name='jack', fullname='Jack Bean', password='gjffdd')
    print jack2
    print jack2.addresses

    jack2.addresses = [
        Address2(email_address='jack@google.com'),
        Address2(email_address='j25@yahoo.com')]

    session.add(jack2)
    session.commit()

    """
    print jack2
    print jack2.addresses
    """
    print "______________"
    jack3 = session.query(User2).get(1)
    print jack3
    print jack3.addresses
    print "______________"
    print session.query(Address2).filter(
        Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count()
    print "______________"
    print session.query(Address2).count()
    print "______________"
    print session.query(Address2).filter(
        Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).all()

    del jack3.addresses[1]
    print "______________"
    print jack3
    print jack3.addresses
    print "______________"
    print session.query(Address2).count()

    session.delete(jack3)

    print session.query(Address2).count()




import sqlalchemy as sa
import sqlalchemy.orm as orm

class Database(object):
    def __init__(self, database_name="stocks.db"):
        self.database_name = database_name
        self.metadata = sa.MetaData('sqlite:///'+database_name)
        try:
            self.load_database()
        except:
            self._create_database()

        # Mappings
        orm.clear_mappers()
        #orm.mapper(Stock, self.stock_table)
        orm.mapper(Dividends, self.div_hist_table)
        orm.mapper(Stock, self.stock_table, properties=dict(
            dividend_history=orm.relation(Dividends, secondary=self.div_hist_table, backref='stock')))#,
            #notes=orm.relation(Notes, secondary=self.notes_table, backref='stock') ))
        #orm.mapper(Notes, self.notes_table)

        # Create Session
        Session = orm.sessionmaker()
        self.session = Session()


    def load_database(self):
        self.stock_table = sa.Table('stock', self.metadata, autoload=True)
        self.div_hist_table = sa.Table('dividend_history', self.metadata, autoload=True)
        #self.notes_table = sa.Table('stock_note', self.metadata, autoload=True)



    def create_database(self):
        self.metadata = None
        self.metadata = sa.MetaData('sqlite:///'+self.database_name)
        self._create_database()
        # Mappings
        orm.clear_mappers()
        #orm.mapper(Stock, self.stock_table)
        orm.mapper(Dividends, self.div_hist_table)
        orm.mapper(Stock, self.stock_table, properties=dict(
            dividend_history=orm.relation(Dividends, secondary=self.div_hist_table, backref='stock')))#,
            #notes=orm.relation(Notes, secondary=self.notes_table, backref='stock') ))
        #orm.mapper(Notes, self.notes_table)



    def _create_database(self):
        self.stock_table = sa.Table(
            # table name and metadata
            'stock', self.metadata,
            # Company Data - Rarely updates
            sa.Column('id', sa.Integer, primary_key=True, index=True),
            sa.Column('symbol', sa.Unicode(5), unique=True, nullable=False, index=True),
            sa.Column('company_name', sa.Text, default=''),
            sa.Column('industry', sa.Text, default=''),
            sa.Column('sector', sa.Text, default=''),
            sa.Column('created', sa.DateTime, default=datetime.datetime.now),
            sa.Column('company_start', sa.DateTime),
            sa.Column('num_full_time_employees', sa.Integer),
            sa.Column('company_data_last_updated', sa.DateTime, default=datetime.datetime.now),
            # Start Key Stats - updates more often
            sa.Column('key_stats_last_updated', sa.DateTime, default=datetime.datetime.now),
            # Key Stats - Valuation
            sa.Column('peg_ratio', sa.Float),
            # Key Stats - Financial Highlights - Income
            sa.Column('most_recent_qrt', sa.DateTime),
            sa.Column('qrt_revenue_growth', sa.Float),
            sa.Column('gross_profit', sa.Float),
            sa.Column('ebitda', sa.Float),
            sa.Column('net_income', sa.Float),
            sa.Column('qrt_earnings_growth', sa.Float),
            sa.Column('eps', sa.Float),
            # Key Stats - Balance sheet
            sa.Column('total_cash', sa.Float),
            sa.Column('total_debt', sa.Float),
            sa.Column('current_ratio', sa.Float),
            sa.Column('book_value_per_share', sa.Float),
            # Key Stats - Cash Flow
            sa.Column('operating_cash_flow', sa.Float),
            sa.Column('levered_free_cash', sa.Float),
            # Key Stats - Trading
            sa.Column('beta', sa.Float),
            sa.Column('num_shares', sa.Float),
            sa.Column('shares_short', sa.Float),
            # Key Stats - Dividends
            sa.Column('forward_div', sa.Float),
            sa.Column('trailing_div', sa.Float),
            sa.Column('last_dividend_date', sa.Float),
            sa.Column('last_ex_dividend_date', sa.Float))


        self.div_hist_table = sa.Table(
            'dividend_history', self.metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('stock_id', sa.Integer, sa.ForeignKey('stock.id')),
            sa.Column('dividend_date', sa.DateTime, nullable=False),
            sa.Column('dividend', sa.Float, nullable=False),
            # For CEF distributions
            sa.Column('nav_at_dist', sa.Float, nullable=True),
            sa.Column('long_term_cap_gains', sa.Float, nullable=True),
            sa.Column('short_term_cap_gains', sa.Float, nullable=True),
            sa.Column('dividend_income', sa.Float, nullable=True),
            sa.Column('return_of_capital', sa.Float, nullable=True))


        self.notes_table = sa.Table(
            'stock_note', self.metadata,
            sa.Column('id', sa.Integer, primary_key=True, index=True),
            sa.Column('stock_id', None, sa.ForeignKey('stock.id'), index=True),
            sa.Column('note', sa.Text),
            sa.Column('url', sa.Text))

        self.metadata.create_all()



class Stock(object):
    dividend_history = []
#    notes = []
class Dividends(object): pass
class Notes(object): pass




def test_database():
    data = Database()
    stock1 = Stock()
    dividend1 = Dividends()
    dividend2 = Dividends()
    dividends = []

    stock1.company_name = "The Company"
    stock1.symbol = u'TC'
    stock1.eps = 1.5
    stock1.forward_div = 2.6

    dividend1.dividend = .45
    dividend1.date = '4/5/2015'
    dividends.append(dividend1)
    dividend2.dividend = .44
    dividend2.dividend = '4/5/2014'
    dividends.append(dividend2)
    stock1.dividend_history = dividends

    data.session.add(stock1)
    data.session.commit()
    data.session.flush()
    return data





def sqlalchemy_createdb_tutorial(database_name = "stocksdb.db"):
    metadata = sa.MetaData('sqlite:///'+database_name)

    group_table = sa.Table(
        'tf_group', metadata,
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('group_name', sa.Unicode(16),
        unique=True, nullable=False))

    permission_table = sa.Table(
        'tf_permission', metadata,
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('permission_name', sa.Unicode(16),
        unique=True, nullable=False))

    user_group_table = sa.Table(
        'tf_user_group', metadata,
        sa.Column('user_id', None, sa.ForeignKey('tf_user.id'),
        primary_key=True),
        sa.Column('group_id', None, sa.ForeignKey('tf_group.id'),
        primary_key=True))

    group_permission_table = sa.Table(
        'tf_group_permission', metadata,
        sa.Column('permission_id', None, sa.ForeignKey('tf_permission.id'),
        primary_key=True),
        sa.Column('group_id', None, sa.ForeignKey('tf_group.id'),
        primary_key=True))

    metadata.create_all()
