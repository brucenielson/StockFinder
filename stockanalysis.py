import yahoostockdata
import stockdatabase
import datetime


#KEY_STATS_FIELDS = "symbol, TotalDebt, MarketCap, " \
#         + "OperatingCashFlow, Beta, "\
#         + "DividendDate, Ex_DividendDate, ForwardAnnualDividendRate, "\
#         + "TotalCashPerShare, QtrlyEarningsGrowth, "\
#         + "TotalCash, Revenue, ForwardPE, SharesOutstanding, "\
#         + "TotalDebtEquity"


#QUOTE_FIELDS = "symbol, LastTradePriceOnly, YearLow, YearHigh, DividendShare, " \
#        + "EarningsShare, " \
#        + "BookValue, EPSEstimateNextQuarter, EBITDA, EPSEstimateCurrentYear, EPSEstimateNextYear, " \
#        + "PriceEPSEstimateNextYear, MarketCapitalization, Name, HoldingsValue, DividendPayDate, " \
#        + "ExDividendDate, Currency, DividendYield"


#STOCK_FIELDS = "symbol, Industry, Sector, start, FullTimeEmployees"

#DIVIDEND_HISTORY_FIELDS = "Symbol, Dividends, Date"


#ALL_FIELDS = QUOTE_FIELDS + ", " + KEY_STATS_FIELDS + ", " + STOCK_FIELDS



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
    #snplist = []
    #try:
    #    snplist = stockdatabase.get_wikipedia_snp500_list()
    #except:
    #    snplist = stockdatabase.get_pickled_snp_500_list()

    snplist = stockdatabase.get_wikipedia_snp500_list()
    snp = yahoostockdata.get_combined_data(snplist)
    return snp


def small_test_set():
    stocklist = ['T', 'IBM', 'AAPL', 'GOOG', 'PKO', 'OKS', 'XOM', 'STZ', 'COP']
    stocks = yahoostockdata.get_combined_data(stocklist)
    return stocks




def years_ago(years, from_date=None):
    if from_date is None:
        from_date = datetime.datetime.now()
    try:
        years = int(years)
        return from_date.replace(year=from_date.year - years)
    except:
        # Must be 2/29!
        assert from_date.month == 2 and from_date.day == 29 # can be removed
        return from_date.replace(month=2, day=28, year=from_date.year-years)




# Analyze data in various ways and label it. Input: data object with
# all stocks as output by get_combined_data
def analyze_data(data):
    # loop through each stock and analyze if it's a dividend stock or not
    for stock in data:
        if 'IsDividend' not in data:
            data[stock]['IsDividend'] = is_dividend_stock(data[stock])

        # Further analysis on dividend stocks
        if data[stock]['IsDividend'] == True:
            analyze_dividend_history_yahoo_data(data[stock])



def get_div_acheivers(data, years):
    div_acheiver = {}
    for symbol in data.keys():
        stock = data[symbol]
        if 'YearsOfDividendGrowth' in stock and stock['YearsOfDividendGrowth'] >= years:
            div_acheiver[symbol] = data[symbol]

    return div_acheiver



# Review Dividend History and tags this stock with appropriate tags
def analyze_dividend_history_yahoo_data(stock):
    # Determine how long the stock has paid dividends for
    div_hist = stock['DividendHistory']
    most_recent_div_date = div_hist[0]['Date']
    last_index = len(div_hist)-1
    # If this stock has only one dividend, then abort
    if last_index == 0:
        return

    start_div_date = div_hist[last_index]['Date']
    div_hist_len = ((most_recent_div_date - start_div_date).days / 365.25)
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

    # TODO Calculate a trailing year of dividends not including bonus dividends

    div_growth_start_div = find_start_of_div_growth(div_hist_no_bonus, tot_divs_per_year_no_bonus)
    stock['FirstDividendGrowth'] = div_growth_start_div
    div_growth_start_date = div_growth_start_div['Date']
    stock['DividendGrowthStartDate'] = div_growth_start_date
    div_growth_len = ((most_recent_div_date - div_growth_start_date).days / 365.25)
    stock['YearsOfDividendGrowth'] = div_growth_len

    # Determine Total Growth
    recent_growth = []
    #print div_growth_len
    if div_growth_len >= 1.0:
        most_recent_div = div_hist[0]
        total_div_growth_amt = float(most_recent_div['Dividends']) - float(div_growth_start_div['Dividends'])
        total_div_growth_rate = total_div_growth_amt / float(div_growth_start_div['Dividends'])
        stock['TotalDividendGrowth'] = total_div_growth_rate / div_growth_len
        # Growth amounts by year
        if div_growth_len >= 20.0:
            stock['DividendGrowth20'] = get_div_growth(div_hist, 20) / 20.0
        if div_growth_len >= 15.0:
            stock['DividendGrowth15'] = get_div_growth(div_hist, 15) / 15.0
        if div_growth_len >= 10.0:
            stock['DividendGrowth10'] = get_div_growth(div_hist, 10) / 10.0
        if div_growth_len >= 5.0:
            stock['DividendGrowth5'] = get_div_growth(div_hist, 5) / 5.0
            recent_growth.append(stock['DividendGrowth5'])
        if div_growth_len >= 3.0:
            stock['DividendGrowth3'] = get_div_growth(div_hist, 3) / 3.0
            recent_growth.append(stock['DividendGrowth3'])
        if div_growth_len >= 1.0:
            stock['DividendGrowth1'] = get_div_growth(div_hist, 1)
            recent_growth.append(stock['DividendGrowth1'])

        recent_growth = min(recent_growth)
        stock['RecentGrowth'] = recent_growth


        #print stock['symbol'] + " - " + "Years of Dividends: " + str(stock['YearsOfDividends']) + "; Start of Growth: " + str(div_growth_start_date) + "; Length of Growth: " + str(div_growth_len) + "; Total Div Growth: " + str(total_div_growth_rate)
        #if len(div_hist_no_bonus) < len(div_hist):
        #    print str(len(div_hist)-len(div_hist_no_bonus)) + " bonus dividends"
        #if div_growth_len >= 20.0:
        #    print "20 Year: " + str(stock['DividendGrowth20'])
        #if div_growth_len >= 15.0:
        #    print "15 Year: " + str(stock['DividendGrowth15'])
        #if div_growth_len >= 10.0:
        #    print "10 Year: " + str(stock['DividendGrowth10'])
        #if div_growth_len >= 5.0:
        #    print "5 Year: " + str(stock['DividendGrowth5'])
        #if div_growth_len >= 3.0:
        #    print "3 Year: " + str(stock['DividendGrowth3'])
        #if div_growth_len >= 1.0:
        #    print "1 Year: " + str(stock['DividendGrowth1'])





# Find the dividend amount a number of 'years' ago. Since years is not likely to be exact, find the first dividend
# on or just before that number of years and return it
def get_div_growth(div_hist, years):
    # Get most recent div
    most_recent_div = div_hist[0]
    most_recent_date = most_recent_div['Date']
    # Search for dividend years ago (minus 5 days to try to capture slight differences in date for ex-dividend)
    target_date = years_ago(years, most_recent_date) - datetime.timedelta(days=5) # TODO: was -5 days, but can that be right?
    for div in div_hist:
        div_date = div['Date']
        if div_date <= target_date:
            div_growth_amt = float(most_recent_div['Dividends']) - float(div['Dividends'])
            div_growth_rate = div_growth_amt / float(div['Dividends'])
            return div_growth_rate

    # We didn't find a dividend within the years specified. This might just be because of the anamoly between how we measure years two ways.
    div_hist_len = len(div_hist)
    div = div_hist[div_hist_len-1]
    if abs((target_date - div['Date']).days) <= 15:
        div_growth_amt = float(most_recent_div['Dividends']) - float(div['Dividends'])
        div_growth_rate = div_growth_amt / float(div['Dividends'])
        return div_growth_rate
    else:
        # Raise an error if we don't find the right dividend
        raise Exception("Not enough dividends in dividend history for " + str(years) +".")




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



def get_trailing_dividend(div_hist):
    #start_date = div_hist[0]['Date']
    #end_date = start_date.replace(year=start_date.year - 1)
    pass




def find_start_of_div_growth(div_hist, tot_divs_per_year=[], start=0):
    # Find last_index based total dividends for year: i.e. dividends must be non-zero and equal to or less (i.e. growth) then the previous year

    # If the user didn't pass the divs per year, we create them
    if len(tot_divs_per_year) == 0:
        tot_divs_per_year = create_div_totals_by_year(div_hist)

    # Find last year of yearly dividend growth or last non-zero year
    last = len(tot_divs_per_year)-1
    last_index = len(div_hist)-1
    if last >= 0:
        start_year = tot_divs_per_year[last][0]
        end_year = tot_divs_per_year[0][0]
    else:
        start_div_date = div_hist[last_index]['Date']
        start_year =  start_div_date.year
        end_year = start_year

    last_year_of_growth = 0
    if start_year != end_year:
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
    else:
        last_year_of_growth = start_year

    # We found last year of growth - now translate that to last index to search for growth in: i.e. only search one year beyond last year of growth
    div_hist = [div for div in div_hist if div['Date'].year >= last_year_of_growth-1]
    last_index = len(div_hist)-1

    # Now find last dividend of growth within restricted range
    if start > last_index:
        symbol = div_hist[0]['Symbol']
        raise Exception("Start ("+str(start)+") must not be less than length remaining ("+str(last_index)+") to search for stock: " + str(symbol))
    for i in range(start, last_index): #We don't compare last element to the one before it because it doens't have one before it
        if not(div_hist[i]['Dividends'] >= div_hist[i+1]['Dividends']):
            # We just found where dividend growth (or stability) ended
            div_hist[i]['StartOfDividendGrowth'] = True
            return div_hist[i]


    # if we never find a point where growth begins, then the very first dividend is that starting point
    div_hist[last_index]['StartOfDividendGrowth'] = True
    return div_hist[last_index]





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




def analyze_dividend_history(stock):


        def mark_bonus_dividends(div_hist):
            last_index = len(div_hist)-1
            for i in range(0, last_index-1): # We don't compare last element to the one before it because it doens't have one before it
                if not(div_hist[i].dividend >= div_hist[i+1].dividend):
                    # We just found where dividend growth (or stability) potentially ended
                    # But we need to check to see if the trend can continue if we assume this is a "bonus" dividend
                    if (i+2 < last_index) and (div_hist[i].dividend >= div_hist[i+2].dividend):
                        possible_bonus_div = div_hist[i+1]
                    elif (i+1 < last_index) and (div_hist[i-1].dividend >= div_hist[i+1].dividend):
                        possible_bonus_div = div_hist[i]
                    else:
                        continue

                    # The trend continues if we assume this is a "one off" or "bonus" dividend
                    # However, to be considered such, the following criteria must still be met:
                    # 1. This must be a year with 5 or 13 dividends
                    # 2. There must be no other bonus dividend in this year already
                    year = possible_bonus_div.dividend_date.year
                    year_of_divs = [div for div in div_hist if div.dividend_date.year == year]

                    # This can't be a bonus dividend if the # of dividends for a year isn't 5 or 13
                    is_bonus = False
                    if len(year_of_divs) == 5 or len(year_of_divs) == 13:
                        for div in year_of_divs:
                            if div.is_bonus_dividend == False:
                                is_bonus = True
                                break

                    if is_bonus == True:
                        possible_bonus_div.is_bonus_dividend = True



        def create_div_totals_by_year(div_hist):
            current_year = datetime.datetime.now().year
            last_index = len(div_hist)-1
            start_div_date = div_hist[last_index].dividend_date
            start_year =  start_div_date.year

            divs_per_year = []
            if start_year < current_year:
                for year in list(reversed(range(start_year, current_year+1))):
                    tot_per_year = sum([div.dividend for div in div_hist if div.dividend_date.year == year])
                    divs_per_year.append((year, tot_per_year))

            return divs_per_year



        def find_start_of_div_growth(div_hist, tot_divs_per_year=[], start=0):
            # Find last_index based total dividends for year: i.e. dividends must be non-zero and equal to or less (i.e. growth) then the previous year

            # If the user didn't pass the divs per year, we create them
            if len(tot_divs_per_year) == 0:
                tot_divs_per_year = create_div_totals_by_year(div_hist)

            # Find last year of yearly dividend growth or last non-zero year
            last = len(tot_divs_per_year)-1
            last_index = len(div_hist)-1
            if last >= 0:
                start_year = tot_divs_per_year[last][0]
                end_year = tot_divs_per_year[0][0]
            else:
                start_div_date = div_hist[last_index].dividend_date
                start_year =  start_div_date.year
                end_year = start_year

            last_year_of_growth = 0
            if start_year != end_year:
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
            else:
                last_year_of_growth = start_year

            # We found last year of growth - now translate that to last index to search for growth in: Not sure if I should search one year beyond or not. You get problems either way
            div_hist = [div for div in div_hist if div.dividend_date.year >= last_year_of_growth]

            last_index = len(div_hist)-1

            # Now find last dividend of growth within restricted range
            if start > last_index:
                symbol = div_hist[0].symbol
                raise Exception("Start ("+str(start)+") must not be less than length remaining ("+str(last_index)+") to search for stock: " + str(symbol))
            for i in range(start, last_index): #We don't compare last element to the one before it because it doens't have one before it
                if not(div_hist[i].dividend >= div_hist[i+1].dividend):
                    # We just found where dividend growth (or stability) ended
                    div_hist[i].is_start_of_growth = True
                    return i


            # if we never find a point where growth begins, then the very first dividend is that starting point
            div_hist[last_index].is_start_of_growth = True
            return last_index



        # Determine how long the stock has paid dividends for
        if len(stock.dividends) < 1:
            return
        div_hist = stock.dividends
        #most_recent_div_date = div_hist[0].dividend_date
        today =datetime.datetime.today()
        last_index = len(div_hist)-1
        # If this stock has only one dividend, then abort
        if last_index == 0:
            return

        start_div_date = div_hist[last_index].dividend_date
        div_hist_len = ((today - start_div_date).days / 365.25)
        stock.years_dividends = div_hist_len

        # Determine Dividend Growth

        # "dividend growth" is misleading here. We're really looking for growth or stability.
        # So what this function really does is look for a dividend in the past that is larger than the ones that come after it
        mark_bonus_dividends(div_hist)

        # What's the real dividend per year? (Not including current year because its only partially complete and thus always too low)
        tot_divs_per_year_no_bonus = create_div_totals_by_year(stock.dividends_no_bonus())[1:]
        #tot_divs_per_year = create_div_totals_by_year(div_hist)[1:]

        # TODO Calculate a trailing year of dividends not including bonus dividends

        stock.start_of_div_growth_index = find_start_of_div_growth(stock.dividends_no_bonus(), tot_divs_per_year_no_bonus)
        div_growth_start_div = stock.dividends[stock.start_of_div_growth_index]
        stock.div_growth_start_date = div_growth_start_div.dividend_date
        stock.years_div_growth = ((today - stock.div_growth_start_date).days / 365.25)

        # Create data for target analysis
        stock.recent_div_growth = recent_div_growth(stock)
        stock.projected_growth = projected_growth(stock)
        stock.projected_div_adj = projected_div_adj(stock)
        stock.projected_div = projected_div(stock)
        #stock.target_price = target_price(stock)




def projected_div(stock):
    projected_growth = stock.projected_growth
    if stock.current_div() != None and projected_growth != None:
        return float(stock.current_div()) + (stock.current_div() * stock.projected_growth) * 5.0
    else:
        return None



def projected_div_adj(stock):
    projected_growth = stock.projected_growth
    div_adj = stock.div_adj()
    if stock.current_div() != None and div_adj != None and projected_growth != None:
        return float(div_adj) + (div_adj * projected_growth) * 5.0
    else:
        return None




def projected_growth(stock):
    # If 3 year growth is greater than 1 year growth, then assume "recent growth" will drop off by equivalant amount
    div_growth_1 = stock.div_growth(1)
    div_growth_2 = stock.div_growth(2)
    recent_div_growth = stock.recent_div_growth
    if div_growth_2 != None and div_growth_1 != None and div_growth_2 > div_growth_1:
        diff = div_growth_2 - div_growth_1
        projected_growth = recent_div_growth - diff
        if projected_growth < 0.0:
            return 0.0
        else:
            return projected_growth
    else:
        if recent_div_growth != None:
            return recent_div_growth


def recent_div_growth(stock):
    list_growths = []
    if stock.div_growth(3) != None:
        list_growths.append(stock.div_growth(3))
    if stock.div_growth(2) != None:
        list_growths.append(stock.div_growth(2))
    if stock.div_growth(1) != None:
        list_growths.append(stock.div_growth(1))
    if len(list_growths) > 0:
        return min(list_growths)
    else:
        return None




def get_stock_target_analysis_yahoo_data(data):
    for symbol in data.keys():
        stock = data[symbol]
        stock['ProjectedRateAdjusted'] = 0.0

        if 'DividendShare' in stock and stock['DividendShare'] != None and stock['LastTradePriceOnly'] != None:
            stock['CalcYield'] = float(stock['DividendShare']) / float(stock['LastTradePriceOnly'])

            if 'YearsOfDividendGrowth' in stock and stock['YearsOfDividendGrowth'] >= 1.0:
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



def cef_distribution_analysis(data):
    i = 0
    for symbol in data.keys():
        i+=1
        print str(symbol) + " - " + str(i)
        stock = data[symbol]

        if 'DividendShare' in stock and stock['DividendShare'] != None:
            stock['CalcYield'] = float(stock['DividendShare']) / float(stock['LastTradePriceOnly'])

        # Get distribution / dividend info from Fidelity.com
        try:
            stock['Distributions'] = stockdatabase.get_cef_dividend_info(symbol)
        except:
            stock['Distributions'] = "Failed To Load"

        # Now analyze results: Calculate % return of capital for available years and for total of all available years
        if stock['Distributions'] == []:
            stock['Distributions'] = "N/A"
        else:
            calculate_return_of_capital_totals(stock)




def calculate_return_of_capital_totals(stock):
    distributions = stock['Distributions']
    if str(distributions) == "Failed To Load":
        return

    current_year = datetime.datetime.now().year
    last_index = len(distributions)-1
    start_div_date = distributions[last_index]['Ex-Date']
    start_year =  start_div_date.year
    grand_total = 0
    total_roc = 0

    dist_per_year = []
    if start_year < current_year:
        for year in list(reversed(range(start_year, current_year+1))):
            total_check = sum([div['Distribution Total'] for div in distributions if div['Ex-Date'].year == year])
            income = sum([div['Dividend Income'] for div in distributions if div['Ex-Date'].year == year])
            capital_gains = sum([div['Short-Term Capital Gains'] + div['Long-Term Capital Gains'] for div in distributions if div['Ex-Date'].year == year])
            return_of_capital = sum([div['Return of Capital'] for div in distributions if div['Ex-Date'].year == year])
            total = income + capital_gains + return_of_capital
            if abs(total_check - total) > 0.001:
                raise Exception("Totals do not match for " +str(distributions[0]['Symbol'])+ ". From Website: " + str(total_check) + ". Our Calculations: " + str(total))

            if total > 0.0:
                perc_roc = return_of_capital / total
            else:
                perc_roc = "N/A"

            row = {}
            row[year] = {'total', total}
            row[year] = {'income': income}
            row[year] = {'capital gains': capital_gains}
            row[year] = {'return of capital': return_of_capital}
            row[year] = {'PercentReturnOfCapital':perc_roc}
            dist_per_year.append(row)
            grand_total += total
            total_roc += return_of_capital

    if grand_total > 0.0:
        total_perc_roc = total_roc / grand_total
    else:
        total_perc_roc = "N/A"

    stock['TotalPercentRoC'] = total_perc_roc
    stock['DistributionYearTotals'] = dist_per_year







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




