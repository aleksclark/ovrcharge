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
with open('charges.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=",", quotechar='"')
    for row in reader:
        if keys is None:
            keys = row
        else:
            data = dict(zip(keys, row))

            data['Hospital Referral Region Description'] = data['Hospital Referral Region Description'].replace(" ", "")
            drg_code = data['DRG Definition'][0:3]
            prov_id = data['Provider Id']
            overcharge = (float(data['Average Covered Charges']) - float(data['Average Total Payments'])) / float(data['Average Total Payments'])
            overcharge = int(overcharge * 100)
            #print overcharge
            p.sadd('drg', drg_code)
            p.zadd('drg_names', drg_code, data['DRG Definition'])
            p.set('drg:' + drg_code + ":desc", data['DRG Definition'])
            p.zadd('providers', overcharge, prov_id)
            p.hset('providers:' + prov_id, "name", data['Provider Name'])
            p.hset('providers:' + prov_id, "address", data['Provider Street Address'])
            p.hset('providers:' + prov_id, "city", data['Provider City'])
            p.hset('providers:' + prov_id, "state", data['Provider State'])
            p.hset('providers:' + prov_id, "zip", data['Provider Zip Code'])

            p.sadd('zips', data['Provider Zip Code'])
            # p.zadd('zip:' + data['Provider Zip Code'], data['Provider Zip Code'], prov_id)
            # p.sadd('regions', data['Hospital Referral Region Description'])

            p.set('drg:' + drg_code + ':' + prov_id + ':discharges', data['Total Discharges'])
            p.set('drg:' + drg_code + ':' + prov_id + ':charges', data['Average Covered Charges'])
            p.set('drg:' + drg_code + ':' + prov_id + ':payments', data['Average Total Payments'])
            p.set('drg:' + drg_code + ':' + prov_id + ':overcharge', overcharge)

        counter += 1
        if counter > 100:
            counter = 0
            p.execute()


counter = 0

providers = r.zrange('providers', 0, -1)
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
            r.zadd("drg:" + drg + ":providers", int(overcharge), provider)
    if total_discharges != 0 and total_overcharge != 0:
        avg_overcharge = total_overcharge / total_discharges
    else:
        print "BLAR"
    r.hset("providers:" + provider, "avg_overcharge", avg_overcharge)
    print "done with " + provider + " " + str(avg_overcharge)
        # p.sadd('region:' + data['Hospital Referral Region Description'], prov_id)
        # p.sadd('zip:' + data['Provider Zip Code'], prov_id)
counter = 0


# drgs = r.smembers('drg')

for provider in providers:
    prov_info = r.hgetall('providers:' + provider)
    score = int(prov_info['avg_overcharge'])
    p.zadd('zip:' + prov_info['zip'], score, provider)
    p.zadd('city:' + prov_info['city'], score, provider)
    p.zadd('state:' + prov_info['state'], score, provider)
    p.execute()
