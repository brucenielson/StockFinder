import unittest
from stockdatabase import *
import mock


__author__ = 'bruce'


class test_stock_database(unittest.TestCase):


    @mock.patch('stockdatabase.sqlite3')
    def test_create_database(self, mock_sqlite):
        #try:
        #    os.remove(os.path.dirname(__file__)+"\\unittest.db")
        #except:
        #    pass

        #self.assertFalse(os.path.isfile(os.path.dirname(__file__)+"\\unittest.db"), "Failed to remove the file.")
        create_database("unittest.db")
        mock_sqlite.connect.assert_called_with(os.path.dirname(__file__)+"\\unittest.db")
        #mock_sqlite.complete_statement.assert_called_with('create index labelx on label(label)')

        #self.assertTrue(os.path.isfile(os.path.dirname(__file__)+"\\unittest.db"), "Database file not create.")
        #os.remove(os.path.dirname(__file__)+"\\unittest.db")


    @mock.patch('stockdatabase.lxml.html')
    def test_get_wikipedia_snp500_list(self, mockl_xml_html):
        sl = get_wikipedia_snp500_list()
        mockl_xml_html.parse.assert_called_with('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')



    @mock.patch('stockdatabase.pickle')
    @mock.patch('stockdatabase.get_wikipedia_snp500_list')
    def test_pickle_snp(self, mock_wiki, mock_pickle):
        pickle_snp_500_list()
        mock_wiki.assert_called()
        mock_pickle.dump.assert_called()
        sl = get_pickled_snp_500_list()
        mock_pickle.load.assert_called()






if __name__ == '__main__': # pragma: no cover
    unittest.main()
