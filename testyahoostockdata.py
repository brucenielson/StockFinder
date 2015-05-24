import unittest
import mock
import os
from yahoostockdata import *
import yahoostockdata




# Get Test Data
def get_data():
    import pickle

    f = open(os.path.dirname(__file__)+"\\"+'testdata.txt')
    data = pickle.load(f)
    f.close()
    return data

data = get_data()


fake_quote_data_1 = data['q1t']
fake_quote_data_2 = data['q2t']
fake_test_pass_string = data['q2t']
fake_stock_data_1 = data['s1t']
fake_stock_data_2 = data['s2t']
fake_div_hist_data_1 = data['div1t']
fake_div_hist_data_2 = data['div2t']
fake_div_hist_data_3 = data['div3t']
fake_div_hist_data_4 = data['div4t']

# Result data all prettied up to compare with
result_quote_data_1 =  data['q1r']
result_quote_data_2 = data['q2r']
result_test_data_pass_string =  data['q2r']

result_stock_data_1 =  data['s1r']
result_stock_data_2 = data['s2r']

result_div_hist_data_1 = data['div1r']
result_div_hist_data_2 = data['div2r']
result_div_hist_data_3 = data['div3r']
result_div_hist_data_4 = data['div4r']


__author__ = 'Bruce Nielson'





class test_yahoo_stock_data(unittest.TestCase):

    #Unit Test get_quote_data
    @mock.patch('yahoostockdata.urllib2.urlopen')
    @mock.patch('yahoostockdata.json.loads')
    def test_get_quotes_data(self, mock_json_loads, mock_urllib2_urlopen):
        # Setup mocks
        mock_urllib2_urlopen.return_value = "mock file object"
        mock_json_loads.return_value = fake_quote_data_1

        # Test list of quotes with upper and lowercase symbols
        quote_data = get_quote_data(['aapl', 'T', 'MSFT', 'GOOG'])

        self.failIf(quote_data != result_quote_data_1)

        # Test that if they are not equal, it realizes that they aren't
        temp = quote_data['AAPL'] ["YearLow"]
        quote_data['AAPL'] ["YearLow"] = "Mr. Magoo"
        self.failIf(quote_data == result_quote_data_1)
        quote_data['AAPL'] ["YearLow"] = temp


        # Test trying to get a field that shouldn't be there.
        found = True
        try:
            price = quote_data['AAPL']['TotalDebt']
        except Exception, error:
            found = False
        self.failIf(found == True)



        # Test WMT, which shouldn't be there
        found = True
        try:
            price = quote_data['WMT']['LastTradePriceOnly']
        except Exception, error:
            found = False
        self.failIf(found == True)

        # Test a list of one
        mock_json_loads.return_value = fake_quote_data_2
        quote_data = get_quote_data(['aapl'])
        self.failIf(quote_data != result_quote_data_2)




    #Unit Test __process_symbol_list indirectly since its private
    @mock.patch('yahoostockdata.urllib2.urlopen')
    @mock.patch('yahoostockdata.json.loads')
    def test_process_symbol_list(self, mock_json_loads, mock_urllib2_urlopen):
        # Setup mocks
        mock_urllib2_urlopen.return_value = "mock file object"
        mock_json_loads.return_value = fake_test_pass_string

        # Test list of quotes with upper and lowercase symbols
        quote_data = get_quote_data('aapl')

        success = False
        result = 0
        try:
            result = quote_data['aapl']
        except:
            success = True

        if not (success == True and result == 0):
            self.fail("Found 'aapl' instead of 'AAPL'") # pragma: no cover

        success = False
        result = 0
        try:
            result = quote_data['AAPL']
            success = True
        except: # pragma: no cover
            success = False

        if not (success == True and result == result_quote_data_2['AAPL']):
            self.fail("Found 'aapl' instead of 'AAPL'") # pragma: no cover

        # Setup mocks
        mock_urllib2_urlopen.return_value = "mock file object"
        mock_json_loads.return_value = fake_quote_data_1

        # Test list of quotes in a string instead of a list
        quote_data = get_quote_data("aapl, T, MSFT, GOOG")

        self.failIf(quote_data != result_quote_data_1)

        # straight tests
        result = yahoostockdata._process_symbol_list(["aapl, T, MSFT, goog"])
        correct = ["AAPL, T, MSFT, GOOG"]
        self.failIf(result != correct)
        result = yahoostockdata._process_symbol_list("aapl")
        correct = ["AAPL"]
        self.failIf(result != correct)


    #Unit Test get_stock_data
    @mock.patch('yahoostockdata.urllib2.urlopen')
    @mock.patch('yahoostockdata.json.loads')
    def test_get_stock_data(self, mock_json_loads, mock_urllib2_urlopen):
        # Setup mocks
        mock_urllib2_urlopen.return_value = "mock file object"
        mock_json_loads.return_value = fake_stock_data_1

        # Test list of quotes with upper and lowercase symbols
        data = get_stock_data(['aapl', 'T', 'MSFT', 'GOOG'])

        self.failIf(data != result_stock_data_1)


        # Test WMT, which shouldn't be there
        found = True
        try:
            row = data['WMT']
        except Exception, error:
            found = False
        self.failIf(found == True)

        # Test trying to get a field that shouldn't be there.
        found = True
        try:
            price = data['AAPL']['TrailingPE']
        except Exception, error:
            found = False
        self.failIf(found == True)


        # Setup mocks
        mock_urllib2_urlopen.return_value = "mock file object"
        mock_json_loads.return_value = fake_stock_data_2

        # Test list of quotes with upper and lowercase symbols
        data = get_stock_data(['aapl'])

        self.failIf(data != result_stock_data_2)




    #Unit Test get_dividend_history_data
    @mock.patch('yahoostockdata.urllib2.urlopen')
    @mock.patch('yahoostockdata.json.loads')
    def test_dividend_history_data(self, mock_json_loads, mock_urllib2_urlopen):
        # Setup mocks
        mock_urllib2_urlopen.return_value = "mock file object"
        mock_json_loads.return_value = fake_div_hist_data_1

        # Test list of quotes with upper and lowercase symbols
        data = get_dividend_history_data(['aapl', 'T', 'MSFT'])

        #fields = ALL_DIVIDEND_HISTORY_FIELDS.split(", ")
        #i=0
        #is_equal(data, result_div_hist_data_1)

        self.failIf(data != result_div_hist_data_1)

        # Test WMT, which shouldn't be there
        found = True
        try:
            row = data['WMT']
        except Exception, error:
            found = False
        self.failIf(found == True)


        # Test GOOG which has no dividends. This mimics safe_get_data() and also tests what happens when there are no dividends
        mock_json_loads.return_value = fake_div_hist_data_2
        data = get_dividend_history_data(['GOOG'])
        self.failIf(data != {})



        # Test list of quotes with upper and lowercase symbols
        mock_json_loads.return_value = fake_div_hist_data_3
        data = get_dividend_history_data(['aapl'])

        self.failIf(data != result_div_hist_data_3)

        # Test CNHI which has only one dividend.
        mock_json_loads.return_value = fake_div_hist_data_4
        data = get_dividend_history_data('cnhi')

        self.failIf(data != result_div_hist_data_4)




if __name__ == '__main__': # pragma: no cover
    unittest.main()



