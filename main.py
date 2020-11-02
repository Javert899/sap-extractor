from flask import Flask, render_template, request, make_response, jsonify, redirect, url_for

app = Flask(__name__)


@app.route('/')
def empty_path():
    return redirect(url_for('welcome'))


@app.route('/index.html')
def index():
    return redirect(url_for('welcome'))


@app.route("/welcome")
def welcome():
    response = make_response(render_template('extraction.html'))
    return response


def main():
    app.run(host='0.0.0.0')


if __name__ == "__main__":
    main()
