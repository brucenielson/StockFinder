import yahoostockdata
#import xlwt
import xlsxwriter
import os
import datetime
import stockdatabase
import stockanalysis



def create_div_achievers_list(use_saved_snp=False):
    snp = []
    if use_saved_snp == True:
        snp = stockdatabase.get_pickle_stock_data()

    if snp == []:
        snplist = stockdatabase.get_snp500_list()
        snp = yahoostockdata.get_combined_data(snplist)
        # save most recent for next time
        stockdatabase.pickle_stock_data(snp)

    stockanalysis.analyze_data(snp)
    div_achievers_10 = stockanalysis.get_div_acheivers(snp, 10)
    create_stock_list_worksheet(div_achievers_10)

    return div_achievers_10



def create_custom_stock_list(list=None):
    if list == None:
        # Load from disk
        pass

    data = yahoostockdata.get_combined_data(list)

    stockanalysis.analyze_data(data)
    create_stock_list_worksheet(data)

    return data




column_count = 0
column_list = []

def init_columns():
    global column_count
    column_count = 0
    global column_list
    column_list = []



saved_title_format = None
def set_title_format(title_format):
    global saved_title_format
    saved_title_format = title_format


def add_column(worksheet, col_name, col_value, format=None):
    # Write Label in first row
    global column_count, saved_title_format

    worksheet.write(0, column_count, str(col_name)+":", saved_title_format)
    column_count += 1
    # Create column list for writing out data
    global column_list
    new_col = (col_value, format)
    column_list.append(new_col)




def get_stock_target_analysis(data):
    for symbol in data.keys():
        stock = data[symbol]
        stock['CalcYield'] = float(stock['DividendShare']) / float(stock['LastTradePriceOnly'])

        if stock['YearsOfDividendGrowth'] >= 1.0:
            stock['MostRecentDiv'] = most_recent_div = stock['DividendHistory'][0]
            div_growth_start_div = stock['FirstDividendGrowth']
            stock['TotalDivenendGrowth'] = total_div_growth_amt = float(most_recent_div['Dividends']) - float(div_growth_start_div['Dividends'])
            stock['TotalDividendGrowthRate'] = total_div_growth_rate = total_div_growth_amt / float(div_growth_start_div['Dividends'])
            recent_growth = stock['RecentGrowth']

            # If 3 year growth is greater than 1 year growth, then assume "recent growth" will drop off by equivalant amount
            if 'DividendGrowth3' in stock and 'DividendGrowth1' in stock and stock['DividendGrowth3'] > stock['DividendGrowth1']:
                diff = stock['DividendGrowth3'] - stock['DividendGrowth1']
                projected_growth = recent_growth - diff
                if projected_growth < 0.0:
                    projected_growth = 0.0
            else:
                projected_growth = recent_growth

            # Find Adjusted Dividend

            max_div = max([0, stock['EarningsShare']]) * 0.6 # 60% payout ratio
            stock['AdjustedDividend'] = min([max_div, stock['DividendShare']])
            stock['PayoutRatioWarning'] = (stock['AdjustedDividend'] < stock['DividendShare'])

            # Work out projected Dividend
            stock['ProjectedGrowth'] = projected_growth
            stock['ProjectedDividend'] = projected_div = float(stock['DividendShare']) + (float(stock['DividendShare']) * projected_growth) * 5
            stock['ProjectedRate'] = projected_div / stock['LastTradePriceOnly']

            # Is dividend under earnings pressure? If so, find adjusted dividend growth also
            stock['ProjectedDividendAdjusted'] = projected_div_adj = float(stock['AdjustedDividend']) + (float(stock['AdjustedDividend']  * projected_growth) * 5)
            stock['ProjectedRateAdjusted'] = projected_div_adj / stock['LastTradePriceOnly']


            # Find suggested purchase price
            # We want a dividend that is making at least 5% in 5 years
            stock['TargetPrice'] = min([stock['LastTradePriceOnly'], projected_div_adj / .05])
            stock['PercentToTarget'] = (stock['LastTradePriceOnly'] - stock['TargetPrice']) / stock['LastTradePriceOnly']





def create_stock_list_worksheet(data):
    # Create workbook
    current_date = datetime.datetime.now()
    today_str = current_date.strftime('%Y-%b-%d')
    book = xlsxwriter.Workbook(os.path.dirname(__file__)+"\\spreadsheets\\stocklist"+today_str+".xlsx")

    # Create worksheet with list of dividend stocks with at least 10 years of history
    div_achievers = book.add_worksheet("Dividend Achievers")

    # Create formats
    title_format = book.add_format({'bold': True})
    set_title_format(title_format)
    dollar_format = book.add_format({'num_format': '$#,##0.00'})
    date_format = book.add_format({'num_format': 'dd-mmm-yyyy'})
    percent_format = book.add_format({'num_format': '0.00%'})

    #Pre-Process Data
    get_stock_target_analysis(data)

    # Create Columns
    init_columns()
    add_column(div_achievers, 'Symbol', 'symbol')
    add_column(div_achievers, 'Name', 'Name')
    add_column(div_achievers, 'Last', 'LastTradePriceOnly', dollar_format)
    add_column(div_achievers, 'High', 'YearHigh', dollar_format)
    add_column(div_achievers, 'Low', 'YearLow', dollar_format)
    add_column(div_achievers, 'Years of Divs', 'YearsOfDividends')
    add_column(div_achievers, 'Length Growth', 'YearsOfDividendGrowth')
    add_column(div_achievers, 'Total Growth', 'TotalDividendGrowth', percent_format)
    add_column(div_achievers, 'Recent', 'RecentGrowth', percent_format)
    add_column(div_achievers, 'Projected', 'ProjectedGrowth', percent_format)
    add_column(div_achievers, 'Dividend', 'DividendShare', dollar_format)
    add_column(div_achievers, 'Yield', 'CalcYield', percent_format)
    add_column(div_achievers, 'EPS', 'EarningsShare', dollar_format)
    add_column(div_achievers, 'Adjusted Div:', 'AdjustedDividend', dollar_format)
    add_column(div_achievers, 'Adjusted Div 5 year', 'ProjectedDividendAdjusted', dollar_format)
    add_column(div_achievers, 'Adjusted Yield 5 year', 'ProjectedRateAdjusted', percent_format)
    add_column(div_achievers, 'Div Warn:', 'PayoutRatioWarning')
    add_column(div_achievers, 'Target', 'TargetPrice', dollar_format)
    add_column(div_achievers, 'Target%', 'PercentToTarget', percent_format)

    # Create sort order by projected yield
    sort_order = create_sort_list(data, 'ProjectedRateAdjusted')

    # Write out the sheet
    create_sheet(div_achievers, data, sort_order)

    # Create Excel workbook
    book.close()



def create_sort_list(data, column):
    # Create sort order by projected yield
    sort_order = [(symbol, data[symbol][column]) for symbol in data.keys()]
    sort_order.sort(key=lambda sort_order: sort_order[1], reverse=True)
    sort_order = [item[0] for item in sort_order]
    return sort_order





def create_sheet(worksheet, data, sort_order=None):
    global column_list

    if sort_order == None:
        sort_order = data.keys()

    # Process Data
    i = 1
    for symbol in sort_order:
        stock = data[symbol]
        j = 0
        for column in column_list:
            if column[0] in stock:
                worksheet.write(i, j, stock[column[0]], column[1])
            j += 1
        i+=1




def create_stock_details_worksheet(stock_symbol):
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


