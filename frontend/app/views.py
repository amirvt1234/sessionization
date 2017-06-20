from app import app
from flask import render_template
import json
import redis
# import things
from flask_table import Table, Col
import flask
#from bokeh.embed import components
from bokeh.plotting import figure
#from bokeh.resources import INLINE
#from bokeh.templates import RESOURCES
#from bokeh.util.string import encode_utf8


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



def get_most_engaged():
       response = redisConnection.zrange('EngagementTime',0,10,True,True)
       return [dict(name=x[0], description=str(int(x[1]/1000))) for x in response]

def get_most_viewed():
       response = redisConnection.zrange('ViewerCount',0,10,True,True)
       return [dict(name=x[0], description=str(int(x[1]))) for x in response]


most_viewed = get_most_viewed()
most_engaged = get_most_engaged()

#10_most_engaged = get_most_engaged()

@app.route('/')
@app.route('/index')
def index():
  user = { 'nickname': 'This is my UI!' } # fake user
  table1 = mostviewedtable(most_viewed)
  table2 = mostengagedtabe(most_engaged)
  #tables = [mostviewedtable(most_viewed), mostengagedtabe(most_engaged)]
  titles = ['na', 'Most Viewed', 'Most Engaged']
  return render_template("index.html", title = 'Home', user = user,  tables=[table1, table2], titles=titles)

