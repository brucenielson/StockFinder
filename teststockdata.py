import unittest
import mock
from mock import Mock
from stockdata import *

fake_quote_data_1 = {u'query': {u'count': 3, u'lang': u'en-US', u'results': {u'quote': [{u'YearLow': u'70.5071', u'PEGRatio': u'1.24', u'DividendShare': u'1.8457', u'BookValue': u'19.015', u'ShortRatio': u'1.90', u'PERatio': u'17.44', u'Symbol': u'AAPL', u'PriceSales': u'3.61', u'PriceBook': u'5.92', u'EarningsShare': u'6.45', u'LastTradePriceOnly': u'110.38', u'YearHigh': u'119.75'}, {u'YearLow': u'31.74', u'PEGRatio': u'2.87', u'DividendShare': u'1.84', u'BookValue': u'17.862', u'ShortRatio': u'12.90', u'PERatio': u'10.43', u'Symbol': u'T', u'PriceSales': u'1.35', u'PriceBook': u'1.91', u'EarningsShare': u'3.269', u'LastTradePriceOnly': u'33.59', u'YearHigh': u'37.48'}, {u'YearLow': u'34.63', u'PEGRatio': u'2.66', u'DividendShare': u'1.15', u'BookValue': u'10.923', u'ShortRatio': u'2.40', u'PERatio': u'18.43', u'Symbol': u'MSFT', u'PriceSales': u'4.24', u'PriceBook': u'4.30', u'EarningsShare': u'2.551', u'LastTradePriceOnly': u'46.45', u'YearHigh': u'50.05'}]}, u'created': u'2015-01-02T03:03:20Z'}}
fake_quote_data_2 = {u'query': {u'count': 1, u'lang': u'en-US', u'results': {u'quote': {u'YearLow': u'70.5071', u'PEGRatio': u'1.24', u'DividendShare': u'1.8457', u'BookValue': u'19.015', u'ShortRatio': u'1.90', u'PERatio': u'17.44', u'Symbol': u'AAPL', u'PriceSales': u'3.61', u'PriceBook': u'5.92', u'EarningsShare': u'6.45', u'LastTradePriceOnly': u'110.38', u'YearHigh': u'119.75'}}, u'created': u'2015-01-02T03:17:40Z'}}
quote_url_1 = "http://query.yahooapis.com/v1/public/yql?q=select%20Symbol%2C%20LastTradePriceOnly%2C%20YearLow%2C%20YearHigh%2C%20DividendShare%2C%20EarningsShare%2C%20PERatio%2C%20PriceSales%2C%20PEGRatio%2C%20ShortRatio%2C%20BookValue%2C%20PriceBook%20from%20yahoo.finance.quotes%20where%20symbol%20in%20%28%27AAPL%27%2C%27T%27%2C%27MSFT%27%29&format=json&env=http%3A%2F%2Fdatatables.org%2Falltables.env&callback="


__author__ = 'Bruce Nielson'

class test_stock_data_utilities(unittest.TestCase):


    #Unit Test get_quotes_data
    @mock.patch('stockdata.urllib2.urlopen')
    @mock.patch('stockdata.json.loads')
    #@mock.patch('stockdata.urllib2.addinfourl.read')
    def test_get_quotes_data(self, mock_json_loads, mock_urllib2_urlopen):#, mock_urllib2_addinfourl_read):
        # Setup mocks
        mock_urllib2_urlopen.return_value = "mock file object"
        mock_json_loads.return_value = fake_quote_data_1

        #mock_urllib2_addinfourl_read.return_value = "mock addinfourl object"

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
        mock_json_loads.return_value = fake_quote_data_2
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



    #Unit Test get_stocks_data
    def test_dividend_history(self):
    # Test list of quotes with upper and lowercase symbols
        data = get_dividend_history_data(['aapl', 'WMT', 'MSFT'])

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
        data = get_dividend_history_data(['aapl'])

        try:
            hist = data['AAPL']['DividendHistory']
        except Exception, error:
            self.fail("Failure: %s" % error)

        data = get_dividend_history_data(['AAPL'])

        try:
            hist = data['AAPL']['DividendHistory']
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




    def test_get_stock_and_dividend_history_data(self):
                # Test list of quotes with upper and lowercase symbols
        data = get_stock_and_dividend_history_data(['aapl', 'WMT', 'MSFT',])

        value = 0.0

        try:
            value = data['AAPL']['start']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(type(value) == type(datetime.datetime))

        try:
            value = data['WMT']['start']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(type(value) == type(datetime.datetime))

        try:
            value = data['MSFT']['start']
        except Exception, error:
            self.fail("Failure: %s" % error)

        self.failIf(type(value) == type(datetime.datetime))

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
        data = get_stock_and_dividend_history_data(['aapl'])

        try:
            value = data['AAPL']['start']
        except Exception, error:
            self.fail("Failure: %s" % error)

        try:
            hist = data['AAPL']['DividendHistory']
        except Exception, error:
            self.fail("Failure: %s" % error)


        data = get_stock_and_dividend_history_data(['AAPL'])

        try:
            value = data['AAPL']['start']
        except Exception, error:
            self.fail("Failure: %s" % error)

        try:
            hist = data['AAPL']['DividendHistory']
        except Exception, error:
            self.fail("Failure: %s" % error)



if __name__ == '__main__':
    unittest.main()



