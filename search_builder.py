import redis
import csv

# Key, type, value
# drg, set, list of drgs
# drg:(drg_code):desc, value, drg descriptions
# providers, set, list of providers
# providers:(provider_id), hash, provider information
# avg_overcharge (field of above hash), value, volume-weighted average overcharge %
# zips, set, list of zips
# drg:(drg_code):(provider_id):discharges, value, total discharges per provider/drg
# drg:(drg_code):(provider_id):charges, value, Average Covered Charges (i.e. what the hospital charged)
# drg:(drg_code):(provider_id):payments, value, Average Total Payments (i.e. what medicare actually paid)
# drg:(drg_code):(provider_id):overcharge, value, % overcharge ((charges - payments) / payments) * 100
# drb:(drg_code):providers, sorted set, providers ranked by % overcharge for this drg
# zip:(zip), sorted set, providers in a zip ranked by avg_overcharge
# city:(city), sorted set, providers in a city ranked by avg_overcharge
# state:(state), sorted set, providers in a state ranked by avg_overcharge


r = redis.StrictRedis(host='localhost', port=6379, db=1)
keys = None
p = r.pipeline()
counter = 0

providers = r.zrange('providers', 0, -1)


for provider in providers:

    prov_info = r.hgetall('providers:' + provider)
    agg_string = ""
    for k in prov_info:
        agg_string = agg_string + " " + prov_info[k]
    terms = agg_string.lower().split(" ")
    for term in terms:
        if term != "":
            p.sadd('text:provider:' + term, provider)
    p.execute()

drgs = r.smembers('drg')

for drg in drgs:
    tokens = r.get('drg:' + drg + ":desc").lower().split(" ")
    for token in tokens:
        if token != "":
            p.sadd('text:drg:' + token, drg)
    p.execute()

zips = r.smembers('zips')
for zip_code in zips:
    pref = zip_code[0:3]
    providers = r.zrange('zip:' + zip_code, 0, -1)
    for prov in providers:
        p.sadd('text:zip:' + pref, prov)
    p.execute()
