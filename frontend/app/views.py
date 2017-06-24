from app import app
from flask import render_template
import json
from flask_table import Table, Col
import flask
import time
from flask import Flask, Response
import sys
#import redis
sys.path.append("../utils") # fix me!
import redisdb

# Declare your table
class recentspiders(Table):
    name = Col('Bot ID')
    description = Col('Blocked at')


class mostengagedtabe(Table):
    name = Col('User ID')
    description = Col('Session Time')

x= 22

recent_spiders = redisdb.get_sorted_spiders()
most_engaged   = redisdb.get_most_engaged()

@app.route('/')
@app.route('/index')
def index():
  table1 = recentspiders(recent_spiders)
  table2 = mostengagedtabe(most_engaged)
  return render_template("index.html", table1=table1, table2=table2)
