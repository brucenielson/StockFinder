import stockquotes
import xlwt
import datetime

div_nearx = stockquotes.get_dividend_history_data('nearx')
div_list = div_nearx['NEARX']['DividendHistory']

book = xlwt.Workbook(encoding="utf-8")
sheet1 = book.add_sheet("Dividends")

sheet1.write(0,0, "Symbol")
sheet1.write(0,1, "Date")
sheet1.write(0,2, "Dividend")


i = 0
last_year = datetime.datetime.now().year
date_format = xlwt.XFStyle()
date_format.num_format_str = 'dd-mmm-yyyy'
top = 0
sheet1.col(1).width = (12) * 256
total = 0
totals = []

for div in div_list:
    i += 1
    date = div['Date']
    year = date.year
    if year != last_year:
        totals.append({'dollars': total, 'year': last_year})
        total = 0
        last_year = year
        formula = "SUM(C"+str(top+1)+":C"+str(i)+")"
        sheet1.write(i, 2, xlwt.Formula(formula))
        i += 1
        top = i
    sheet1.write(i, 0, div['Symbol'])
    sheet1.write(i, 1, div['Date'], date_format)
    dollars = div['Dividends']
    sheet1.write(i, 2, dollars)
    total += dollars

i+=1
totals.append({'dollars': total, 'year': year})
formula = "SUM(C"+str(top+1)+":C"+str(i)+")"
sheet1.write(i, 2, xlwt.Formula(formula))



sheet2 = book.add_sheet("Yearly Totals")
sheet2.write(0, 0, "Year")
sheet2.write(0, 1, "Total")
i = 1

for div in reversed(totals):
    sheet2.write(i, 0, div['year'])
    sheet2.write(i, 1, div['dollars'])
    i += 1


book.save("NearX.xls")


"""
x=1
y=2
z=3

list1=[2.34,4.346,4.234]

book = xlwt.Workbook(encoding="utf-8")

sheet1 = book.add_sheet("Sheet 1")

sheet1.write(0, 0, "Display")
sheet1.write(1, 0, "Dominance")
sheet1.write(2, 0, "Test")

sheet1.write(0, 1, x)
sheet1.write(1, 1, y)
sheet1.write(2, 1, z)

sheet1.write(4, 0, "Stimulus Time")
sheet1.write(4, 1, "Reaction Time")

i=4

for n in list1:
    i = i+1
    sheet1.write(i, 0, n)
"""

