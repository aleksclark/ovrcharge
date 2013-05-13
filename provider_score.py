import redis

r = redis.StrictRedis(host='localhost', port=6379, db=1)
keys = None
p = r.pipeline()
counter = 0

providers = r.smembers('providers')
drgs = r.smembers('drg')

for provider in providers:
    total_discharges = 0
    total_overcharge = 0
    keys = []
    for drg in drgs:
        discharges = r.get("drg:" + drg + ":" + provider + ':discharges')
        overcharge = r.get("drg:" + drg + ":" + provider + ':overcharge')
        if discharges is not None and overcharge is not None:
            total_discharges += int(discharges)
            total_overcharge += int(discharges) * int(overcharge)
    if total_discharges != 0 and total_overcharge != 0:
        avg_overcharge = total_overcharge / total_discharges
    else:
        print "BLAR"
    r.hset("providers:" + provider, "avg_overcharge", avg_overcharge)
    print "done with " + provider + " " + str(avg_overcharge)
