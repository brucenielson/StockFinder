from flask import Flask, send_from_directory, jsonify, Response
import stockdatalayer
import json
#from flask.ext.sqlalchemy import SQLAlchemy
#import os
#from models import *

app = Flask(__name__)
#app.config.from_object(os.environ['APP_SETTINGS'])
#db = SQLAlchemy(app)
database = stockdatalayer.Datalayer()

# How to convert joson.dumps to a response
# http://code.runnable.com/UiHUJzdm7P5OAAA3/how-to-set-the-content-type-header-for-a-response-using-flask-for-python-and-json
# Or just give up and do it manually
# https://blogs.gnome.org/danni/2013/03/07/generating-json-from-sqlalchemy-objects/

# Form of $http that worked
# http://fdietz.github.io/recipes-with-angular-js/consuming-external-services/requesting-json-data-with-ajax.html
# http://www.w3schools.com/angular/angular_http.asp

@app.route("/data/<list_code>", methods=['GET'])
def get_results(list_code):
    try:
        results = {}
        results["records"] = database.get_stocks_by_code(list_code.upper())[0:10]
    except Exception as e:
        print 'got error:' + str(e)

    print 'at 1'
    try:
        encoder = stockdatalayer.new_alchemy_encoder(['symbol', 'sector', 'industry'])
        stocks = json.dumps(results, cls=encoder, check_circular=False)
        #print stocks
    except Exception as e:
        print 'got error:' + str(e)

    response = Response(stocks,  mimetype='application/json')
    print response
    return response

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