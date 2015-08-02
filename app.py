from flask import Flask, send_from_directory
#from flask.ext.sqlalchemy import SQLAlchemy
import os

app = Flask(__name__)
#app.config.from_object(os.environ['APP_SETTINGS'])
#db = SQLAlchemy(app)

#from models import *


@app.route('/')
def serve_html():
    return send_from_directory('HTML', 'index.html')


#@app.route('/<name>')
#def hello_name(name):
#    return "Hello {}!".format(name)

if __name__ == '__main__':
    app.run()




#app = Flask(__name__, static_url_path='')

#@app.route('/js/<path:path>')
#def send_js(path):
#    return send_from_directory('js', path)