from app import app
from flask import render_template
import json
import redis

with open('../myconfigs.json', 'r') as f:
    myconfigs = json.load(f)
redisConnection = redis.StrictRedis(host="172.31.53.147", port=6379, db=0)

def get_most_engaged():
       response = redisConnection.zrange('EngagementTime',1,10,True,True)
       return response

@app.route('/')
@app.route('/index')
def index():
  user = { 'nickname': 'Miguelaaaaasdf' } # fake user
  return render_template("index.html", title = 'Home', user = user)
