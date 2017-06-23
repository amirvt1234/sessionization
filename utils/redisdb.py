import redis
import json

with open('../myconfigs.json', 'r') as f:
    myconfigs = json.load(f)
redisConnection = redis.StrictRedis(host="172.31.53.147", port=6379, db=0)

def get_most_engaged():
       response = redisConnection.zrange('EngagementTime',0,11,True,True)
       return [dict(name=x[0], description=str(int(x[1]/1000))) for x in response]

def get_most_viewed():
       response = redisConnection.zrange('ViewerCount',0,10,True,True)
       return [dict(name=x[0], description=str(int(x[1]))) for x in response]

def check_if_spider(key):
       return redisConnection.hget('SPIDERS', key)

def get_all_spider():
       return redisConnection.hgetall('SPIDERS')






