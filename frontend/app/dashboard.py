import redis
import json
from flask import jsonify
#from flask_table import Table
from flask_table import Table, Col


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

most_viewed = get_most_viewed()
#print ss, type(ss)
#spider = get_spiders()
#print spider, type(spider)
#ss = get_total_count()
#print ss, type(ss)
#ss = get_most_engaged()
#print ss, type(ss)

# Declare your table
class ItemTable(Table):
    name = Col('Name')
    description = Col('Description')

items = [dict(name='Name1', description='Description1'),
         dict(name='Name2', description='Description2'),
         dict(name='Name3', description='Description3')]


dummy = [] 
for i in most_viewed:
    print i
    dummy.append(dict(name=i[0], description=str(i[1])))
print dummy[0], type(dummy[0])
print items[0], type(items[0])



#if not spider['39']:
#    print "good"
#else:
#    print 'bad'

"""
ss = get_spiders()
print ss, type(ss)
sss = get_most_engaged()
print sss, type(sss)
gg = get_total_count()
print gg
"""

