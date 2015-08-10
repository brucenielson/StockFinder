from flask import Flask, send_from_directory, jsonify
import stockdatalayer
import time
app = Flask(__name__)
database = stockdatalayer.Datalayer()


@app.route("/data/<list_code>", methods=['GET'])
def get_results(list_code):
    print 'start request'
    start_time = time.clock()
    filter_fields = ['symbol', 'sector', 'industry', 'eps', 'last_price', 'year_low', 'year_high', 'trailing_div', 'company_name', 'div_yield']
    stock_list = database.get_stocks_by_code(list_code.upper())
    database.get_real_time_quotes(stock_list)
    jsonifible = [stock.convert_to_jsonifible(filter_fields) for stock in stock_list]
    response = jsonify(records=jsonifible)
    end_time = time.clock()
    print "Retriving Data: " + str(end_time-start_time) + " seconds"
    return response


def test_get_results():
    start_time = time.clock()
    filter_fields = ['symbol', 'sector', 'industry', 'eps', 'last_price', 'year_low', 'year_high', 'trailing_div', 'company_name', 'div_yield', 'years_div_growth']
    stock_list = database.get_stocks_by_code("SNP")[0:50]
    database.get_real_time_quotes(stock_list)
    jsonifible = [stock.convert_to_jsonifible(filter_fields) for stock in stock_list]
    end_time = time.clock()
    print "Retriving Data: " + str(end_time-start_time) + " seconds"
    return jsonifible



@app.route('/<path:path>')
def serve_html(path):
    result = send_from_directory('HTML', path)
    return result



#@app.route('/<name>')
#def hello_name(name):
#    return "Hello {}!".format(name)



#app = Flask(__name__, static_url_path='')

#@app.route('/js/<path:path>')
#def send_js(path):
#    return send_from_directory('js', path)


if __name__ == '__main__':
    app.run()


