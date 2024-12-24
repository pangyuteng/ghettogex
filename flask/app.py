import os
import sys
import argparse
from flask import (
    Flask, request, render_template, Response,
    redirect, url_for, jsonify,
)


app = Flask(__name__,
    static_url_path='', 
    static_folder='static',
    template_folder='templates',
)

app.config["SECRET_KEY"] = "the random string"


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--port",type=int,default=5000)
    args = parser.parse_args()
    app.run(debug=True,host="0.0.0.0",port=args.port)



"""

docker run -it -p 5000:5000 -u $(id -u):$(id -g) \
    -w $PWD -v /mnt:/mnt \
    fi-flask bash

python app.py

"""