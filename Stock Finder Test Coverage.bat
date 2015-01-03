del .coverage
del htmlcov /f /q
nosetests --with-coverage
python -m coverage html
start /d "C:\Program Files\Internet Explorer" IEXPLORE.EXE C:\Python27\StockFinder\htmlcov\index.html
start /d "C:\Program Files\Internet Explorer" IEXPLORE.EXE C:\Python27\StockFinder\htmlcov\stockdata.html
