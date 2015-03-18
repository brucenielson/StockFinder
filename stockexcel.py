import stockquotes
import xlwt
import os

def create_stock_worksheet(stock_symbol):

    # Get current data for stock symbol
    quote = stockquotes.get_quote_data(stock_symbol)[stock_symbol]
    key_stats = stockquotes.get_key_stats_data(stock_symbol)[stock_symbol]
    stock_info = stockquotes.get_stock_data(stock_symbol)[stock_symbol]
    stock_div_hist = stockquotes.get_dividend_history_data(stock_symbol)
    div_list = stock_div_hist[stock_symbol]['DividendHistory']
    current_price = quote['LastTradePriceOnly']
    year_low = quote['YearLow']

    # Create formats
    date_format = xlwt.XFStyle()
    date_format.num_format_str = 'dd-mmm-yyyy'
    dollar_format = xlwt.XFStyle()
    dollar_format.num_format_str = "[$$-409]#,##0.00;-[$$-409]#,##0.00"
    percent_format = xlwt.XFStyle()
    percent_format.num_format_str = "0.00%" #"0.00\\%"
    title_format = xlwt.XFStyle()
    font = xlwt.Font()
    font.name = 'Times New Roman'
    font.height = 18 * 20 # size * twips (=20 per point)
    title_format.font = font

    # Create workbook
    stock_book = xlwt.Workbook(encoding="utf-8")

    # Create worksheet with basic stock quote information
    quote_sheet = stock_book.add_sheet("Stock Quote")

    # Basic company and quote info
    quote_sheet.write(0,0, "Symbol:")
    quote_sheet.write(0,1, stock_symbol)
    quote_sheet.write(0,2, "Name:")
    quote_sheet.write(0,3, quote['Name'])
    quote_sheet.write(0,4, "Price:")
    quote_sheet.write(0,5, current_price, dollar_format)
    quote_sheet.write(0,6, "Year Low:")
    quote_sheet.write(0,7, year_low, dollar_format)
    quote_sheet.write(0,8, "Year High:")
    quote_sheet.write(0,9, quote['YearHigh'], dollar_format)
    # Continue company info
    quote_sheet.write(1,0, "Industry:")
    quote_sheet.write(1,1, stock_info['Industry'])
    quote_sheet.write(1,2, "Sector:")
    quote_sheet.write(1,3, stock_info['Sector'])
    quote_sheet.write(1,4, "Founded:")
    quote_sheet.write(1,5, stock_info['start'], date_format)
    quote_sheet.write(1,6, "# of Employees:")
    quote_sheet.write(1,7, stock_info['FullTimeEmployees'])
    # Key Stats
    quote_sheet.write(3, 0, "Key Statistics:", title_format)
    quote_sheet.write(4, 0, "Dividend:")
    quote_sheet.write(4, 1, key_stats['ForwardAnnualDividendRate'], dollar_format)
    quote_sheet.write(4, 2, quote['DividendShare'], dollar_format)
    quote_sheet.write(5, 0, "Earnings:")
    quote_sheet.write(5, 1, key_stats['Revenue']) #B6
    quote_sheet.write(6, 0, "# Shares:")
    quote_sheet.write(6, 1, key_stats['SharesOutstanding']) #B7
    quote_sheet.write(7, 0, "EPS:")
    formula = "B6/B7"
    quote_sheet.write(7, 1, xlwt.Formula(formula))
    quote_sheet.write(7, 2, quote['EarningsShare'])
    quote_sheet.write(7, 3, key_stats['ForwardAnnualDividendRate'])



    # create workseet with all dividends
    div_sheet = stock_book.add_sheet("Dividends")
    div_sheet.write(0,0, "Symbol")
    div_sheet.write(0,1, "Date")
    div_sheet.write(0,2, "Dividend")
    div_sheet.write(0,3, "Year Total")


    i = 0
    last_year = div_list[0]['Date'].year
    top = 0
    div_sheet.col(1).width = (12) * 256
    total = 0
    totals = []

    # Loop through each dividend
    # Create dividend worksheet
    for div in div_list:
        i += 1
        date = div['Date']
        year = date.year
        # Create a summary for the last year
        if year != last_year:
            totals.append({'dollars': total, 'year': last_year})
            total = 0
            last_year = year
            formula = "SUM(C"+str(top+1)+":C"+str(i)+")"
            div_sheet.write(i-1, 3, xlwt.Formula(formula), dollar_format)
            top = i
        div_sheet.write(i, 0, div['Symbol'])
        div_sheet.write(i, 1, div['Date'], date_format)
        dollars = div['Dividends']
        div_sheet.write(i, 2, dollars, dollar_format)
        total += dollars

    # Final yearly total
    totals.append({'dollars': total, 'year': year})
    formula = "SUM(C"+str(top+1)+":C"+str(i+1)+")"
    div_sheet.write(i,3, xlwt.Formula(formula), dollar_format)


    # Create yearly totals worksheet
    year_tot_sheet = stock_book.add_sheet("Yearly Totals")
    year_tot_sheet.write(0, 0, "Year")
    year_tot_sheet.write(0, 1, "Total")
    year_tot_sheet.write(0, 2, "Yield")
    year_tot_sheet.write(0, 3, "Low Yield")
    year_tot_sheet.write(0, 5, "Last Price:")
    year_tot_sheet.write(0, 7, "Year Low:")
    # write stock price
    year_tot_sheet.write(0, 6, current_price, dollar_format)
    year_tot_sheet.write(0, 8, year_low, dollar_format)

    # Create yearly totals
    i = 1
    for div in reversed(totals):
        year_tot_sheet.write(i, 0, div['year'])
        year_tot_sheet.write(i, 1, div['dollars'], dollar_format)
        formula = "B"+str(i+1)+"/$G$1)"
        year_tot_sheet.write(i, 2, xlwt.Formula(formula), percent_format)
        formula = "B"+str(i+1)+"/$I$1"
        year_tot_sheet.write(i, 3, xlwt.Formula(formula), percent_format)
        i += 1

    # Create Excel workbook
    stock_book.save(os.path.dirname(__file__)+"\\spreadsheets\\"+stock_symbol+".xls")

