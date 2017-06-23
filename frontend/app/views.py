from app import app
from flask import render_template
import json
from flask_table import Table, Col
import flask
import time
from flask import Flask, Response


with open('../myconfigs.json', 'r') as f:
    myconfigs = json.load(f)
redisConnection = redis.StrictRedis(host="172.31.53.147", port=6379, db=0)



# Declare your table
class mostviewedtable(Table):
    name = Col('User ID')
    description = Col('Number of Clicks')


class mostengagedtabe(Table):
    name = Col('User ID')
    description = Col('Session Time')

x= 189

def get_most_engaged():
       response = redisConnection.zrange('EngagementTime',0,11,True,True)
       return [dict(name=x[0], description=str(int(x[1]/1000))) for x in response]



def get_most_viewed():
       response = redisConnection.zrange('ViewerCount',0,10,True,True)
       return [dict(name=x[0], description=str(int(x[1]))) for x in response]


most_viewed = get_most_viewed()
most_engaged = get_most_engaged()

#10_most_engaged = get_most_engaged()
def stream_template(template_name, **context):
    # http://flask.pocoo.org/docs/patterns/streaming/#streaming-from-templates
    app.update_template_context(context)
    t = app.jinja_env.get_template(template_name)
    rv = t.stream(context)
    # uncomment if you don't need immediate reaction
    return rv


@app.route('/')
@app.route('/index')
def index():
  user = { 'nickname': 'This is my UI'} # fake user
  def g():
     time.sleep(.1)  # an artificial delay
     #table1 = mostviewedtable(most_viewed)
     #table2 = mostengagedtabe(most_engaged)
     table1 = most_viewed
     table2 = most_engaged
     yield table1, table2
     #yield 1, 2
  table1 = mostviewedtable(most_viewed)
  table2 = mostengagedtabe(most_engaged)
  tables = [mostviewedtable(most_viewed), mostengagedtabe(most_engaged)]
  titles = ['na', 'Most Viewed', 'Most Engaged']
  return render_template("index.html", title = 'Home', user = user,  tables=[table1, table2], titles=titles)
  #return Response(stream_template('index.html', title = 'Home', user = user,  tables=g(), titles=titles))
  #return Response(stream_template('salam.html', data=g()))
