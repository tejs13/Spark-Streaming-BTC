import ast
import random
from collections import OrderedDict

from flask import Flask, jsonify, request
from flask_cors import CORS
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


app = Flask(__name__)

dataValues = []
categoryValues = []

BTC_DATA = {'vol':0, 'trans_fees':0, "total_hash":0}

tags = {}

# CORS(app)
cors = CORS(app, resources={r"/*": {"origins": "*"}})


@app.route("/")
def home():
    return "This is Blank"


@app.route('/refresh-data', methods=['GET'])
def refresh_data():
    global dataValues, categoryValues, BTC_DATA
    # print("labels now: " + str(dataValues))
    # print("data now: " + str(categoryValues))
    d = {"vol": BTC_DATA['vol'], "trans_fees": BTC_DATA['trans_fees'], "total_hash": BTC_DATA["total_hash"]}

    # static to debug
    # d = {"vol": 10023 ,"trans_fees": random.randint(10, 40)}
    return jsonify(data=d)


@app.route('/updateData', methods=['POST'])
def update_data():
    global tags, dataValues, categoryValues, BTC_DATA


    data = ast.literal_eval(request.data.decode("utf-8"))
    BTC_DATA = data
    # print(f"labels received: {str(categoryValues)}")
    print(f"data received: {BTC_DATA} --- {type(BTC_DATA)}")
    return "success", 201



if __name__ == "__main__":

    app.run(host='localhost', port=5001, debug=True)
