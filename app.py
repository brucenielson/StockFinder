from flask import Flask, send_from_directory, jsonify
import stockdatalayer
app = Flask(__name__)
database = stockdatalayer.Datalayer()


@app.route("/data/<list_code>", methods=['GET'])
def get_results(list_code):
    filter_fields = ['symbol', 'sector', 'industry', 'eps', 'last_price', 'year_low', 'year_high', 'projected_div']
    stock_list = database.get_stocks_by_code(list_code.upper())[0:10]
    for stock in stock_list:
        stock.get_quote()
    jsonifible = [stock.convert_to_jsonifible(filter_fields) for stock in stock_list]
    response = jsonify(records=jsonifible)
    return response


def test_get_results():
    filter_fields = ['symbol', 'sector', 'industry', 'eps', 'last_price', 'year_low', 'year_high', 'projected_div']
    stock_list = database.get_stocks_by_code('SNP')[0:10]
    for stock in stock_list:
        stock.get_quote()
    jsonifible = [stock.convert_to_jsonifible(filter_fields) for stock in stock_list]
    #response = jsonify(records=jsonifible)
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


