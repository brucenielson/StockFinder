import yahoostockdata
#import xlwt
import xlsxwriter
import os


def create_stock_worksheet(stock_symbol):
    stock_symbol = stock_symbol.upper()

    # Get current data for stock symbol
    quote = yahoostockdata.get_quote_data(stock_symbol)[stock_symbol]
    #key_stats = yahoostockdata.get_key_stats_data(stock_symbol)[stock_symbol]
    stock_info = yahoostockdata.get_stock_data(stock_symbol)[stock_symbol]
    stock_div_hist = yahoostockdata.get_dividend_history_data(stock_symbol)
    div_list = stock_div_hist[stock_symbol]['DividendHistory']
    current_price = quote['LastTradePriceOnly']
    year_low = quote['YearLow']

    # Create workbook
    stock_book = xlsxwriter.Workbook(os.path.dirname(__file__)+"\\spreadsheets\\"+stock_symbol+".xlsx")


    # Create formats
    title_format = stock_book.add_format({'bold': True})
    title_format.set_size(18) # size * twips (=20 per point)
    dollar_format = stock_book.add_format({'num_format': '$#,##0.00'})
    date_format = stock_book.add_format({'num_format': 'dd-mmm-yyyy'})
    percent_format = stock_book.add_format({'num_format': '0.00%'})

    # Create worksheet with basic stock quote information
    quote_sheet = stock_book.add_worksheet("Stock Quote")

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
    #quote_sheet.write(4, 1, key_stats['ForwardAnnualDividendRate'], dollar_format)
    quote_sheet.write(4, 2, quote['DividendShare'], dollar_format)
    quote_sheet.write(5, 0, "Earnings:")
    #quote_sheet.write(5, 1, key_stats['Revenue']) #B6
    quote_sheet.write(6, 0, "# Shares:")
    #quote_sheet.write(6, 1, key_stats['SharesOutstanding']) #B7
    quote_sheet.write(7, 0, "EPS:")
    quote_sheet.write(7, 1, "=B6/B7")
    quote_sheet.write(7, 2, quote['EarningsShare'])
    #quote_sheet.write(7, 3, key_stats['ForwardAnnualDividendRate'])



    # create workseet with all dividends
    div_sheet = stock_book.add_worksheet("Dividends")
    div_sheet.write(0,0, "Symbol")
    div_sheet.write(0,1, "Date")
    div_sheet.write(0,2, "Dividend")
    div_sheet.write(0,3, "Year Total")


    i = 0
    last_year = div_list[0]['Date'].year
    top = 0
    div_sheet.set_column('B:B', 12)
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
            div_sheet.write(i-1, 3, "=SUM(C"+str(top+1)+":C"+str(i)+")", dollar_format)
            top = i
        div_sheet.write(i, 0, div['Symbol'])
        div_sheet.write(i, 1, div['Date'], date_format)
        dollars = div['Dividends']
        div_sheet.write(i, 2, dollars, dollar_format)
        total += dollars

    # Final yearly total
    totals.append({'dollars': total, 'year': year})
    div_sheet.write(i,3, "=SUM(C"+str(top+1)+":C"+str(i+1)+")", dollar_format)


    # Create yearly totals worksheet
    year_tot_sheet = stock_book.add_worksheet("Yearly Totals")
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
        year_tot_sheet.write(i, 2,"=B"+str(i+1)+"/$G$1", percent_format)
        year_tot_sheet.write(i, 3, "=B"+str(i+1)+"/$I$1", percent_format)
        i += 1


    # Create Dividend Yearly Chart
    chart = stock_book.add_chart({'type': 'line'})
    # Add a series to the chart
    chart.add_series({'values': '=\'Yearly Totals\'!$B$2:$B$'+str(i-1),
                        'categories': '=\'Yearly Totals\'!$A$2:$A$'+str(i-1),
                        'name': 'Div/Year',
                        'marker': {'type': 'diamond'},})
    year_tot_sheet.insert_chart('F3', chart)


    # Create Excel workbook
    stock_book.close()


