import redis
import json

rediskeys = ['EngagementTime', 'ViewerCount', 'SORTEDSPIDERS']

with open('../myconfigs.json', 'r') as f:
    myconfigs = json.load(f)
redisConnection = redis.StrictRedis(host="172.31.53.147", port=6379, db=0)

def get_most_engaged():
    response = redisConnection.zrange('EngagementTime',0,11,True,True)
    return [dict(name=x[0], description=str(int(x[1]/1000))) for x in response]

def get_sorted_spiders():
    response = redisConnection.zrange('SORTEDSPIDERS',0,10,True,True)
    return [dict(name=x[0], description=str(int(x[1]/1000))) for x in response]

def get_most_viewed():
    response = redisConnection.zrange('ViewerCount',0,10,True,True)
    return [dict(name=x[0], description=str(int(x[1]))) for x in response]

def check_if_spider(key):
    return redisConnection.hget('SPIDERS', key)

def get_all_spider():
    return redisConnection.hgetall('SPIDERS')

def flush_all_keys():
    return redisConnection.flushall()

def set_expire_time(expiretime):
    for key in rediskeys:
        if redisConnection.ttl(key) == -1 or redisConnection.ttl(key) == -2:
        #if redisConnection.ttl(key) == -1:
            redisConnection.expire(key, expiretime)
    






