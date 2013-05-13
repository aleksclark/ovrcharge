import redis

r = redis.StrictRedis(host='localhost', port=6379, db=1)
keys = None
p = r.pipeline()
counter = 0

providers = r.smembers('providers')
# drgs = r.smembers('drg')

for provider in providers:
    prov_info = r.hgetall('providers:' + provider)
    score = int(prov_info['avg_overcharge'])
    p.zadd('zip:' + prov_info['zip'], score, provider)
    p.zadd('city:' + prov_info['city'], score, provider)
    p.zadd('state:' + prov_info['state'], score, provider)
    p.execute()
