#####################################################################################
# Filename       : bitstamp_webapi.py
# Author         : Paul Jamieson
# Created        : 01/08/2021
# Edited         : 01/08/2021
# Python Version : 3.7.7
# Purpose        : WebApi to control on server system
#
# MIT License
#
# Copyright (c) 2021 Paul Jamieson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#####################################################################################
from markupsafe import escape
from flask import Flask, url_for
app = Flask(__name__)


@app.route('/')
def hello_world():
    return "Index Page"


@app.route('/hello')
def hello():
    return "<h2>hello world!!</h2>"


@app.route('/hello/<name>')
def named(name):
    return "<h3>Hello %s!</h3>" % name


@app.route('/userid/<int:userid>')
def show_id(userid):
    return "<h2>Userid: %d</h2>" % userid


@app.route('/path/<path:subpath>')
def show_subpath(subpath):
    return "<h2>subpath %s</h2>" % escape(subpath)


@app.errorhandler(404)
def page_not_found(error):
    return "<p>This route does not exist.</p> <p>%s</p>" % error


if __name__ == "__main__":
    app.run()
