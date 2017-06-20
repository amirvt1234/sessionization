import redis
import json
from flask import jsonify


with open('../../myconfigs.json', 'r') as f:
    myconfigs = json.load(f)
redisConnection = redis.StrictRedis(host="172.31.53.147", port=6379, db=0)

def get_most_engaged():
       response = redisConnection.zrange('EngagementTime',1,10,True,True)
       return response

def get_most_viewed():
       response = redisConnection.zrange('ViewerCount',0,10,True,True)
       return response

def get_total_count():
    fromredis = redisConnection.hgetall('TOTAL_COUNT')
    #jsonredis = [{"name": x[0]}]
    return fromredis



def get_spiders():
    fromredis = redisConnection.hgetall('SPIDERS')
    #jsonredis = [{"name": x[0]}]
    return fromredis

ss = get_most_viewed()
print ss, type(ss)
spider = get_spiders()
print spider, type(spider)
ss = get_total_count()
print ss, type(ss)
ss = get_most_engaged()
print ss, type(ss)



if not spider['39']:
    print "good"
else:
    print 'bad'

"""
ss = get_spiders()
print ss, type(ss)
sss = get_most_engaged()
print sss, type(sss)
gg = get_total_count()
print gg
"""

