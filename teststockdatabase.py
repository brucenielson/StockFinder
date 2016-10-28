import unittest
from stockdatabase import *
import mock


__author__ = 'bruce'

class test_stock_database(unittest.TestCase):

    @mock.patch('stockdatabase.sqlite3')
    def test_create_database(self, mock_sqlite):
        create_database("unittest.db")
        mock_sqlite.connect.assert_called() # _with(os.path.dirname(__file__)+"\\unittest.db") <--this used to work


    @mock.patch('stockdatabase.lxml.html')
    def test_get_snp500_list(self, mock_xml_html):
        sl = get_snp500_list()
        mock_xml_html.parse.assert_called_with('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')




if __name__ == '__main__': # pragma: no cover
    unittest.main()
