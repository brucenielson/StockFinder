import unittest
from stockdata import *

__author__ = 'Bruce Nielson'

class test_stock_data_utilities(unittest.TestCase):



    #Unit Test get_stocks_data
    def test_dividend_history(self):
    # Test list of quotes with upper and lowercase symbols
        data = get_dividend_history(['aapl', 'WMT', 'MSFT'])

        hist = []
        try:
            hist = data['AAPL']['DividendHistory']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(len(hist) <= 0.0)


        try:
            hist = data['MSFT']['DividendHistory']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(len(hist) <= 0.0)

        found = True
        try:
            hist = data['WMT']['DividendHistory']
        except Exception, error:
            found = True

        self.failIf(found == False)

        # Test a list of one, both upper and lowercase
        data = get_dividend_history(['aapl'])

        try:
            hist = data['AAPL']['DividendHistory']
        except Exception, error:
            self.fail("Failure: %s" % error)

        data = get_dividend_history(['AAPL'])

        try:
            hist = data['AAPL']['DividendHistory']
        except Exception, error:
            self.fail("Failure: %s" % error)


    #Unit Test get_quotes_data
    def test_get_quotes_data(self):
        # Test list of quotes with upper and lowercase symbols
        quote_data = get_quotes_data(['aapl', 'T', 'MSFT'])

        price = 0.0

        try:
            price = quote_data['AAPL']['LastTradePriceOnly']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(price <= 0.0)

        try:
            price = quote_data['T']['LastTradePriceOnly']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(price <= 0.0)

        try:
            price = quote_data['MSFT']['LastTradePriceOnly']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(price <= 0.0)

        found = True
        try:
            price = quote_data['WMT']['LastTradePriceOnly']
        except Exception, error:
            found = False

        self.failIf(found == True)

        # Test a list of one, both upper and lowercase
        quote_data = get_quotes_data(['aapl'])

        try:
            price = quote_data['AAPL']['LastTradePriceOnly']
        except Exception, error:
            self.fail("Failure: %s" % error)

        quote_data = get_quotes_data(['AAPL'])

        try:
            price = quote_data['AAPL']['LastTradePriceOnly']
        except Exception, error:
            self.fail("Failure: %s" % error)


    #Unit Test get_key_stats_data
    def test_get_key_stats_data(self):
        # Test list of quotes with upper and lowercase symbols
        data = get_key_stats_data(['aapl', 'T', 'MSFT'])

        value = 0.00

        try:
            value = data['AAPL']['TotalDebt']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(value <= 0.0)

        try:
            value = data['T']['TotalDebt']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(value <= 0.0)

        try:
            value = data['MSFT']['TotalDebt']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(value <= 0.0)

        found = True
        try:
            value = data['WMT']['TotalDebt']
        except Exception, error:
            found = False

        self.failIf(found == True)

        # Test a list of one, both upper and lowercase
        data = get_key_stats_data(['aapl'])

        try:
            value = data['AAPL']['TotalDebt']
        except Exception, error:
            self.fail("Failure: %s" % error)

        data = get_key_stats_data(['AAPL'])

        try:
            value = data['AAPL']['TotalDebt']
        except Exception, error:
            self.fail("Failure: %s" % error)


    #Unit Test get_stocks_data
    def test_get_stocks_data(self):
        # Test list of quotes with upper and lowercase symbols
        data = get_stocks_data(['aapl', 'T', 'MSFT'])

        value = 0.0

        try:
            value = data['AAPL']['start']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(type(value) == type(datetime.datetime))

        try:
            value = data['T']['start']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(type(value) == type(datetime.datetime))

        try:
            value = data['MSFT']['start']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(type(value) == type(datetime.datetime))

        found = True
        try:
            value = data['WMT']['start']
        except Exception, error:
            found = False

        self.failIf(found == True)

        # Test a list of one, both upper and lowercase
        data = get_stocks_data(['aapl'])

        try:
            value = data['AAPL']['start']
        except Exception, error:
            self.fail("Failure: %s" % error)

        data = get_stocks_data(['AAPL'])

        try:
            value = data['AAPL']['start']
        except Exception, error:
            self.fail("Failure: %s" % error)




if __name__ == '__main__':
    unittest.main()



