from flask import Flask, render_template, request, redirect, url_for, Response
import redis

app = Flask(__name__)
r = redis.StrictRedis(host='localhost', port=6379, db=1)

@app.route("/", methods=['GET'])
def index():
    return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
