import yahoostockdata
import stockdatabase
import datetime

KEY_STATS_FIELDS = "symbol, TotalDebt, MarketCap, " \
         + "OperatingCashFlow, Beta, "\
         + "DividendDate, Ex_DividendDate, ForwardAnnualDividendRate, "\
         + "TotalCashPerShare, QtrlyEarningsGrowth, "\
         + "TotalCash, Revenue, ForwardPE, SharesOutstanding, "\
         + "TotalDebtEquity"


QUOTE_FIELDS = "symbol, LastTradePriceOnly, YearLow, YearHigh, DividendShare, " \
        + "EarningsShare, " \
        + "BookValue, EPSEstimateNextQuarter, EBITDA, EPSEstimateCurrentYear, EPSEstimateNextYear, " \
        + "PriceEPSEstimateNextYear, MarketCapitalization, Name, HoldingsValue, DividendPayDate, " \
        + "ExDividendDate, Currency"


STOCK_FIELDS = "symbol, Industry, Sector, start, FullTimeEmployees"

DIVIDEND_HISTORY_FIELDS = "Symbol, Dividends, Date"


ALL_FIELDS = QUOTE_FIELDS + ", " + KEY_STATS_FIELDS + ", " + STOCK_FIELDS



"""
def get_data(symbol_list):
    quote_data = yahoostockdata.get_quote_data(symbol_list)
    stats_data = stockdata.retrieve_key_stats_data(symbol_list)
    stored_date = stockdatabase.retrieve_stock_list_from_db()
    div_hist_data = stockdatabase.get_dividend_history_data(symbol_list)

    data = combine_data(quote_data, stats_data, stored_date, div_hist_data)
    return data
"""


def large_test_set():
    snplist = stockdatabase.get_wikipedia_snp500_list()
    snp = yahoostockdata.get_combined_data(snplist)
    return snp


def small_test_set():
    stocklist = ['T', 'IBM', 'AAPL', 'GOOG', 'PKO', 'OKS', 'XOM', 'STZ', 'COP']
    stocks = yahoostockdata.get_combined_data(stocklist)
    return stocks



# Analyze data in various ways and label it. Input: data object with
# all stocks as output by get_combined_data
def analyze_data(data):
    # loop through each stock and analyze if it's a dividend stock or not
    for stock in data:
        if 'IsDividend' not in data:
            data[stock]['IsDividend'] = is_dividend_stock(data[stock])

        # Further analysis on dividend stocks
        if data[stock]['IsDividend'] == True:
            analyze_dividend_history(data[stock])

    div_acheiver_10 = [data[stock] for stock in data if 'YearsOfDividendGrowth' in data[stock] and data[stock]['YearsOfDividendGrowth'] >= 10.0]
    for div in div_acheiver_10:
        print div['symbol'] + " - " + "Years of Dividends: " + str(div['YearsOfDividends']) + "; Start of Growth: " + str(div['DividendGrowthStartDate']) + "; Length of Growth: " + str(div['YearsOfDividendGrowth'])


# Review Dividend History and tags this stock with appropriate tags
def analyze_dividend_history(stock):
    # Determine how long the stock has paid dividends for
    div_hist = stock['DividendHistory']
    most_recent_div_date = div_hist[0]['Date']
    last_index = len(div_hist)-1
    # If this stock has only one dividend, then abort
    if last_index == 0:
        return

    start_div_date = div_hist[last_index]['Date']
    div_hist_len = ((most_recent_div_date - start_div_date).days / 365.0)
    stock['YearsOfDividends'] = div_hist_len

    # Determine Dividend Growth
    # "dividend growth" is misleading here. We're really looking for growth or stability.
    # So what this function really does is look for a dividend in the past that is larger than the ones that come after it
    mark_bonus_dividends(div_hist)
    # make a list of dividends without bonuses
    div_hist_no_bonus = [div for div in div_hist if not ('IsBonus' in div and div['IsBonus']==True)]
    stock['DividendHistoryNoBonus'] = div_hist_no_bonus

    # What's the real dividend per year? (Not including current year because its only partially complete and thus always too low)
    tot_divs_per_year_no_bonus = create_div_totals_by_year(div_hist_no_bonus)[1:]
    stock['TotalDividendsPerYearNoBonus'] = tot_divs_per_year_no_bonus
    tot_divs_per_year = create_div_totals_by_year(div_hist)[1:]
    stock['TotalDividendsPerYear'] = tot_divs_per_year

    div_growth_start_date = find_start_of_div_growth(div_hist_no_bonus, tot_divs_per_year_no_bonus)
    stock['DividendGrowthStartDate'] = div_growth_start_date
    div_growth_len = ((most_recent_div_date - div_growth_start_date).days / 365.0)
    stock['YearsOfDividendGrowth'] = div_growth_len

    #print stock['symbol'] + " - " + "Years of Dividends: " + str(stock['YearsOfDividends']) + "; Start of Growth: " + str(div_growth_start_date) + "; Length of Growth: " + str(div_growth_len)
    #if len(div_hist_no_bonus) < len(div_hist):
    #    print str(len(div_hist)-len(div_hist_no_bonus)) + " bonus dividends"




def create_div_totals_by_year(div_hist):
    current_year = datetime.datetime.now().year
    last_index = len(div_hist)-1
    start_div_date = div_hist[last_index]['Date']
    start_year =  start_div_date.year

    divs_per_year = []
    if start_year < current_year:
        for year in list(reversed(range(start_year, current_year+1))):
            tot_per_year = sum([div['Dividends'] for div in div_hist if div['Date'].year == year])
            divs_per_year.append((year, tot_per_year))

    return divs_per_year




def find_start_of_div_growth(div_hist, tot_divs_per_year=[], start=0):
    # Find last_index based total dividends for year: i.e. dividends must be non-zero and equal to or less (i.e. growth) then the previous year

    # If the user didn't pass the divs per year, we create them
    if len(tot_divs_per_year) == 0:
        tot_divs_per_year = create_div_totals_by_year(div_hist)

    # Find last year of yearly dividend growth or last non-zero year
    last = len(tot_divs_per_year)-1
    start_year = tot_divs_per_year[last][0]
    end_year = tot_divs_per_year[0][0]

    last_year_of_growth = 0
    years = list(reversed(range(start_year, end_year+1)))
    for i in range(0, len(years)):
        # Continue searching while we're dealing with a non-zero year
        if tot_divs_per_year[i][1] > 0.0:
            # Check if we're on final year yet
            if i < len(years)-1:
                div_this_year = tot_divs_per_year[i][1]
                div_next_year = tot_divs_per_year[i+1][1]
                if not(div_this_year >= div_next_year):
                    # next years dividend ends growth phase
                    last_year_of_growth = tot_divs_per_year[i][0]
                    break
            else: # Else i = final year, so we're already on last year
                last_year_of_growth = tot_divs_per_year[i][0]
                break
        else: # Else we found a zero dividend year
            # This year has no dividends, so previous year is last year
            last_year_of_growth = tot_divs_per_year[i-1][0]
            break

    # We found last year of growth - now translate that to last index to search for growth in: i.e. only search one year beyond last year of growth
    div_hist = [div for div in div_hist if div['Date'].year >= last_year_of_growth-1]
    last_index = len(div_hist)-1

    # Now find last dividend of growth within restricted range
    if start >= last_index:
        raise Exception("Start must not be less than length remaining to search.")
    for i in range(start, last_index): #We don't compare last element to the one before it because it doens't have one before it
        if not(div_hist[i]['Dividends'] >= div_hist[i+1]['Dividends']):
            # We just found where dividend growth (or stability) ended
            return div_hist[i]['Date']


    # if we never find a point where growth begins, then the very first dividend is that starting point
    return div_hist[last_index]['Date']





def mark_bonus_dividends(div_hist):
    last_index = len(div_hist)-1
    for i in range(0, last_index-1): # We don't compare last element to the one before it because it doens't have one before it
        if not(div_hist[i]['Dividends'] >= div_hist[i+1]['Dividends']):
            # We just found where dividend growth (or stability) potentially ended
            # But we need to check to see if the trend can continue if we assume this is a "bonus" dividend
            if (i+2 < last_index) and (div_hist[i]['Dividends'] >= div_hist[i+2]['Dividends']):
                possible_bonus_div = div_hist[i+1]
            elif (i+1 < last_index) and (div_hist[i-1]['Dividends'] >= div_hist[i+1]['Dividends']):
                possible_bonus_div = div_hist[i]
            else:
                continue

            # The trend continues if we assume this is a "one off" or "bonus" dividend
            # However, to be considered such, the following criteria must still be met:
            # 1. This must be a year with 5 or 13 dividends
            # 2. There must be no other bonus dividend in this year already
            year = possible_bonus_div['Date'].year
            year_of_divs = [div for div in div_hist if div['Date'].year == year]

            # This can't be a bonus dividend if the # of dividends for a year isn't 5 or 13
            is_bonus = False
            if len(year_of_divs) == 5 or len(year_of_divs) == 13:
                for div in year_of_divs:
                    if not('IsBonus' in div and div['IsBonus'] == True):
                        is_bonus = True
                        break

            if is_bonus == True:
                possible_bonus_div['IsBonus'] = True




# Does this stock have divident rows?
def is_dividend_stock(stock_data_row):
    if (type(stock_data_row) != type(dict())) or ('symbol' not in stock_data_row):
        raise Exception("Parameter 'stock_data' must be a dictionary of data for a single stock")

    has_div_hist = ('DividendHistory' in stock_data_row and len(stock_data_row['DividendHistory']) > 0)

    paid_recent = False
    if has_div_hist == True:
        last_div_paid = stock_data_row['DividendHistory'][0]['Date']
        days_since = (datetime.datetime.now() - last_div_paid).days
        if days_since <= 365:
            paid_recent = True

    return has_div_hist and paid_recent



# Pass this function a single row of standardized format stock data (i.e. that which comes out of
# get_combined_data() and it will determine if this is a dividend stock or not
def has_consistent_divendend_stock_data(stock_data_row):
    if (type(stock_data_row) != dict) or 'Symbol' not in stock_data_row:
        raise Exception("Parameter 'stock_data' must be a dictionary of data for a single stock")

    has_key_stats = not stock_data_row['NoKeyStats']
    forward_div = float(stock_data_row['ForwardAnnualDividendRate'])
    has_forward_div = not forward_div == 0.00 or forward_div == "N/A"
    has_div_hist = ('DividendHistory' in stock_data_row)
    error_string = "Bad data for Symbol: " + stock_data_row['Symbol'] + " - has_key_stats: " + str(has_key_stats) + "; forward_div: " + str(forward_div) + \
                   "; has_forward_div: " + str(has_forward_div) + "; has_div_hist: " + str(has_div_hist) + ". "

    div_share = float(stock_data_row['DividendShare'])
    div_date = stock_data_row['DividendDate']
    ex_div = stock_data_row['Ex_DividendDate']

    # key stats available
    if has_key_stats:
        if has_div_hist and has_forward_div:
            # This is a dividend stock with history and plan to pay another
            if (div_share > 0.00 and forward_div > 0.00 and div_date != None and ex_div != None):
                return True
            else:
                raise Exception(error_string + "This stock should have a dividend share, a forward dividend rate, a dividend date, and an ex dividend date.")

        elif not has_forward_div and has_div_hist:
            # This is supposed to be a former dividend stock that cut its dividend

            # Yahoo sometimes contains an old Ex_DividendDate / DividendDate and sometimes doesn't for stocks
            # that cut their dividends. So don't check Ex_DividendDate for this case.
            if (div_share == 0.00 and forward_div == 0.00):
                return False
            else:
                raise Exception(error_string + "This stock should not have a dividend share nor a forward dividend rate.")

        elif has_forward_div and not has_div_hist:
            # This is a stock that has no dividends in the past, but is forecasting one
            if (div_share == 0.00 and forward_div > 0.00 and div_date == None and ex_div == None):
                return True
            else:
                raise Exception(error_string + "This stock should not have a dividend share, dividend date, or ex dividend date, but should have a forward dividend rate.")

        else: # not has_forward_div and not has_div_hist:
            # This is a stock that has no dividend history nor is it forecasting one
            diff = datetime.timedelta(0)
            if (ex_div != None): diff = datetime.datetime.now() - ex_div
            if (div_share == 0.00 and forward_div == 0.00 and div_date == None and (ex_div == None or diff >= datetime.timedelta(10*365))):
                return False
            else:
                raise Exception(error_string + "This stock should not have a dividend share, forward dividend rate, or dividend date. "\
                        + "It should also not have an ex dividend rate or it should be older than 10 years old.")


    else: # No key stats available, so only have a dividend share and div history
        if stock_data_row['DividendShare'] > 0.00 and has_div_hist:
            return True
        elif not (stock_data_row['DividendShare'] > 0.00 and has_div_hist):
            return False
        else:
            raise Exception("Dividend Share and Div History does not match.")




