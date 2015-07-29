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


from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
import datetime as dt
from sqlalchemy.orm import relationship, backref, sessionmaker
Base = declarative_base()


def initialize_datalayer(database_name="test.db"):
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
    new_field = Column(String)




def test_database_funcs(database_name="test.db"):
    session = initialize_datalayer(database_name)
    stock1 = Stock()
    stock1.company_name = "Company Name"
    stock1.symbol = "FOO"
    stock1.industry = "Weasle Raising"
    stock1.eps = 1.5
    stock1.forward_div = 2.6

    session.add(stock1)
    session.commit()

    stock2 = session.query(Stock).filter_by(symbol='FOO').one()
    print stock2.new_field



def add_column(engine, table, column):
  table_name = table.description
  column_name = column.compile(dialect=engine.dialect)
  column_type = column.type.compile(engine.dialect)
  engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % (table_name, column_name, column_type))
# http://stackoverflow.com/questions/7300948/add-column-to-sqlalchemy-table
#column = Column('new column', String(100), primary_key=True)
#add_column(engine, column)



import sqlite3, os
def get_schema(database_name='test.db'):
    con = sqlite3.connect(database_name)
    with open('dump.sql', 'w') as f:
        for line in con.iterdump():
            f.write('%s\n' % line)


def get_schema2(database_name='test.db'):
    con = sqlite3.connect(database_name)
    cursor = con.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cursor.fetchall())



def get_schema3(database_name='test.db'):
    con = sqlite3.connect(database_name)
    cursor = con.cursor()
    meta = cursor.execute("PRAGMA table_info('Job')")
    print meta
    for r in meta:
        print r



def get_schema4(database_name='test.db'):
    con = sqlite3.connect(database_name)
    _iterdump(con)

# Mimic the sqlite3 console shell's .dump command
# Author: Paul Kippes <kippesp@gmail.com>

def _iterdump(connection):
    """
    Returns an iterator to the dump of the database in an SQL text format.

    Used to produce an SQL dump of the database.  Useful to save an in-memory
    database for later restoration.  This function should not be called
    directly but instead called from the Connection method, iterdump().
    """

    cu = connection.cursor()
    yield('BEGIN TRANSACTION;')

    # sqlite_master table contains the SQL CREATE statements for the database.
    q = """
        SELECT name, type, sql
        FROM sqlite_master
            WHERE sql NOT NULL AND
            type == 'table'
        """
    schema_res = cu.execute(q)
    for table_name, type, sql in schema_res.fetchall():
        if table_name == 'sqlite_sequence':
            yield('DELETE FROM sqlite_sequence;')
        elif table_name == 'sqlite_stat1':
            yield('ANALYZE sqlite_master;')
        elif table_name.startswith('sqlite_'):
            continue
        # NOTE: Virtual table support not implemented
        #elif sql.startswith('CREATE VIRTUAL TABLE'):
        #    qtable = table_name.replace("'", "''")
        #    yield("INSERT INTO sqlite_master(type,name,tbl_name,rootpage,sql)"\
        #        "VALUES('table','%s','%s',0,'%s');" %
        #        qtable,
        #        qtable,
        #        sql.replace("''"))
        else:
            yield('%s;' % sql)

        # Build the insert statement for each row of the current table
        res = cu.execute("PRAGMA table_info('%s')" % table_name)
        column_names = [str(table_info[1]) for table_info in res.fetchall()]
        q = "SELECT 'INSERT INTO \"%(tbl_name)s\" VALUES("
        q += ",".join(["'||quote(" + col + ")||'" for col in column_names])
        q += ")' FROM '%(tbl_name)s'"
        query_res = cu.execute(q % {'tbl_name': table_name})
        for row in query_res:
            yield("%s;" % row[0])

    # Now when the type is 'index', 'trigger', or 'view'
    q = """
        SELECT name, type, sql
        FROM sqlite_master
            WHERE sql NOT NULL AND
            type IN ('index', 'trigger', 'view')
        """
    schema_res = cu.execute(q)
    for name, type, sql in schema_res.fetchall():
        yield('%s;' % sql)

    yield('COMMIT;')



import sqlite3

def getTableDump(db_file, table_to_dump):
    conn = sqlite3.connect(':memory:')
    cu = conn.cursor()
    cu.execute("attach database '" + db_file + "' as attached_db")
    cu.execute("select sql from attached_db.sqlite_master "
               "where type='table' and name='" + table_to_dump + "'")
    sql_create_table = cu.fetchone()[0]
    cu.execute(sql_create_table);
    cu.execute("insert into " + table_to_dump +
               " select * from attached_db." + table_to_dump)
    conn.commit()
    cu.execute("detach database attached_db")
    sql =  "\n".join(conn.iterdump()).encode('ascii')
    cu.execute(sql)

    #TABLE_TO_DUMP = 'table_to_dump'
    #DB_FILE = 'db_file'

    #sql = getTableDump(DB_FILE, TABLE_TO_DUMP).encode('ascii')
    #cu.execute(sql)
    #print "done"


def add_new_column(table_name, new_column, column_type, default_val=None, database_name='test.db'):
    # Sebastian Raschka, 2014
    # Adding a new column to an existing SQLite database

    sqlite_file = database_name    # name of the sqlite database file
    #table_name = 'my_table_2'	# name of the table to be created
    #id_column = 'my_1st_column' # name of the PRIMARY KEY column
    #new_column1 = 'my_2nd_column'  # name of the new column
    #new_column2 = 'my_3rd_column'  # name of the new column
    #column_type = 'TEXT' # E.g., INTEGER, TEXT, NULL, REAL, BLOB
    #default_val = 'Hello World' # a default value for the new column rows

    # Connecting to the database file
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    if default_val == None:
        # A) Adding a new column without a row value
        c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
                .format(tn=table_name, cn=new_column, ct=column_type))
    else:
        # B) Adding a new column with a default row value
        c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct} DEFAULT '{df}'"\
                .format(tn=table_name, cn=new_column, ct=column_type, df=default_val))

    # Committing changes and closing the connection to the database file
    conn.commit()
    conn.close()



# http://stackoverflow.com/questions/2103274/sqlalchemy-add-new-field-to-class-and-create-corresponding-column-in-table/8462508#8462508
import logging
import re

import sqlalchemy
from sqlalchemy import MetaData, Table#, exceptions
import sqlalchemy.engine.ddl

_new_sa_ddl = sqlalchemy.__version__.startswith('0.7')


def create_and_upgrade(engine, metadata):
    """For each table in metadata, if it is not in the database then create it.
    If it is in the database then add any missing columns and warn about any columns
    whose spec has changed"""
    db_metadata = MetaData()
    db_metadata.bind = engine

    for model_table in metadata.sorted_tables:
        try:
            db_table = Table(model_table.name, db_metadata, autoload=True)
        except: #exceptions.NoSuchTableError:
            pass
            #logging.info('Creating table %s' % model_table.name)
            #model_table.create(bind=engine)
        else:
            if _new_sa_ddl:
                ddl_c = engine.dialect.ddl_compiler(engine.dialect, None)
            else:
                # 0.6
                ddl_c = engine.dialect.ddl_compiler(engine.dialect, db_table)
            # else:
                # 0.5
                # ddl_c = engine.dialect.schemagenerator(engine.dialect, engine.contextual_connect())

            logging.debug('Table %s already exists. Checking for missing columns' % model_table.name)

            model_columns = _column_names(model_table)
            db_columns = _column_names(db_table)

            to_create = model_columns - db_columns
            to_remove = db_columns - model_columns
            to_check = db_columns.intersection(model_columns)

            for c in to_create:
                model_column = getattr(model_table.c, c)
                logging.info('Adding column %s.%s' % (model_table.name, model_column.name))
                assert not model_column.constraints, \
                    'Arrrgh! I cannot automatically add columns with constraints to the database'\
                        'Please consider fixing me if you care!'
                model_col_spec = ddl_c.get_column_specification(model_column)
                sql = 'ALTER TABLE %s ADD %s' % (model_table.name, model_col_spec)
                engine.execute(sql)

            # It's difficult to reliably determine if the model has changed
            # a column definition. E.g. the default precision of columns
            # is None, which means the database decides. Therefore when I look at the model
            # it may give the SQL for the column as INTEGER but when I look at the database
            # I have a definite precision, therefore the returned type is INTEGER(11)

            for c in to_check:
                model_column = model_table.c[c]
                db_column = db_table.c[c]
                x =  model_column == db_column

                logging.debug('Checking column %s.%s' % (model_table.name, model_column.name))
                model_col_spec = ddl_c.get_column_specification(model_column)
                db_col_spec = ddl_c.get_column_specification(db_column)

                model_col_spec = re.sub('[(][\d ,]+[)]', '', model_col_spec)
                db_col_spec = re.sub('[(][\d ,]+[)]', '', db_col_spec)
                db_col_spec = db_col_spec.replace('DECIMAL', 'NUMERIC')
                db_col_spec = db_col_spec.replace('TINYINT', 'BOOL')

                if model_col_spec != db_col_spec:
                    logging.warning('Column %s.%s has specification %r in the model but %r in the database' %
                                       (model_table.name, model_column.name, model_col_spec, db_col_spec))

                if model_column.constraints or db_column.constraints:
                    # TODO, check constraints
                    logging.debug('Column constraints not checked. I am too dumb')

            for c in to_remove:
                model_column = getattr(db_table.c, c)
                logging.warning('Column %s.%s in the database is not in the model' % (model_table.name, model_column.name))


def _column_names(table):
    # Autoloaded columns return unicode column names - make sure we treat all are equal
    return set((unicode(i.name) for i in table.c))