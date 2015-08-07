from flask import Flask, send_from_directory, jsonify
import stockdatalayer
import json
app = Flask(__name__)
database = stockdatalayer.Datalayer()

@app.route("/data/<list_code>", methods=['GET'])
def get_results(list_code):
    filter_fields = ['symbol', 'sector', 'industry']
    stock_list = [stock.convert_to_jsonifible(filter_fields) for stock in database.get_stocks_by_code(list_code.upper())]

    response = jsonify(records=stock_list)
    return response



def test_code():
    list_code = "SNP"
    try:
        results = database.get_stocks_by_code(list_code.upper())[0:10]
    except Exception as e:
        print 'got error:' + str(e)

    print 'at 1'
    try:
        stocks = json.dumps(results, cls=stockdatalayer.encoder, check_circular=False)
        #print stocks
    except Exception as e:
        print 'got error:' + str(e)

    return stocks
    print 'at 2'
    print stocks
    stocks_list = list(stocks)
    try:
        json_result = jsonify(results=stocks_list)
    except Exception as e:
        print 'got error:' + str(e)
    print 'JSON'
    print json_result
    return json_result



@app.route('/<path:path>')
def serve_html(path):
    result = send_from_directory('HTML', path)
    return result



#@app.route('/<name>')
#def hello_name(name):
#    return "Hello {}!".format(name)

if __name__ == '__main__':
    app.run()




#app = Flask(__name__, static_url_path='')

#@app.route('/js/<path:path>')
#def send_js(path):
#    return send_from_directory('js', path)