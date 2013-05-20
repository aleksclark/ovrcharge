from flask import Flask, render_template, request, redirect, url_for, Response
import redis
import re
app = Flask(__name__)
r = redis.StrictRedis(host='localhost', port=6379, db=1)

@app.route("/", methods=['GET'])
def index():
    drg_list = r.zrange('drg_names', 0, -1)
    drgs = []
    for drg in drg_list:
        drgs.append({'code': drg[0:3], 'name': drg})

    return render_template('index.html', drgs=drgs)

@app.route("/about", methods=['GET'])
def about():
    return render_template('about.html')


@app.route("/search", methods=['GET'])
def newsearch():
    search_string = request.args.get('search_string')
    is_zip = re.match('^\d{5}', search_string)
    if is_zip:
        key = is_zip.string[0:3]
        providers = r.smembers('text:zip:' + key)
        provider_info = []

        for prov in providers:
            prov_id = prov
            info = r.hgetall('providers:' + prov_id)
            provider_info.append({'id': prov_id, 'name': info['name'], 'city': info['city'], 'state': info['state'], 'score': info['avg_overcharge']})
        return render_template('list.html', providers=provider_info, drgs=[])

    else:
        tokens = search_string.split(" ")
        keys = []
        for token in tokens:
            keys.append("text:provider:" + token)

        providers = r.sinter(keys)

        provider_info = []
        for prov in providers:
            prov_id = prov
            info = r.hgetall('providers:' + prov_id)
            provider_info.append({'id': prov_id, 'name': info['name'], 'city': info['city'], 'state': info['state'], 'score': info['avg_overcharge']})

        keys = []
        for token in tokens:
            keys.append("text:drg:" + token)

        drgs = r.sinter(keys)
        keys = []
        for drg in drgs:
            keys.append('drg:' + drg + ':desc')

        drg_descs = r.mget(keys, None)
        drgs = dict(zip(drgs, drg_descs))

        return render_template('list.html', providers=provider_info, drgs=drgs)

@app.route("/providers/<string:prov_id>/")
def show_provider(prov_id):
    info = r.hgetall('providers:' + prov_id)
    drg_list = r.zrange('drg_names', 0, -1)
    #print len(drg_list)
    drgs = []
    for drg in drg_list:
        row = {}
        row['code'] = drg[0:3]
        row['name'] = drg
        row['discharges'] = r.get('drg:' + row['code'] + ':' + prov_id + ':discharges')
        if row['discharges'] is not None:
            row['charges'] = r.get('drg:' + row['code'] + ':' + prov_id + ':charges')
            row['payments'] = r.get('drg:' + row['code'] + ':' + prov_id + ':payments')
            row['overcharge'] = r.get('drg:' + row['code'] + ':' + prov_id + ':overcharge')
            drgs.append(row)
    return render_template('provider.html', info=info, drgs=drgs)
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
