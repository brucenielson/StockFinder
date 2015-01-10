cd c:\python27\stockfinder\
del .coverage
del htmlcov /f /q
c:\python27\scripts\nosetests --with-coverage
C:\Python27\python -m coverage html
start /d "C:\Program Files\Internet Explorer" IEXPLORE.EXE C:\Python27\StockFinder\htmlcov\index.html
start /d "C:\Program Files\Internet Explorer" IEXPLORE.EXE C:\Python27\StockFinder\htmlcov\stockquotes.html
