from flask import Flask, render_template, request, redirect, url_for, Response
import redis

app = Flask(__name__)
r = redis.StrictRedis(host='localhost', port=6379, db=1)

@app.route("/", methods=['GET'])
def index():
    drg_list = r.zrange('drg_names', 0, -1)
    drgs = []
    for drg in drg_list:
        drgs.append({'code': drg[0:3], 'name': drg})

    return render_template('index.html', drgs=drgs)

@app.route("/textsearch", methods=['POST'])
def textsearch():
    search = request.form['search']
    tokens = search.lower().split(" ")
    keys = []
    for token in tokens:
        keys.append("text:" + token)
    #print keys
    providers = r.sinter(keys)
    #print providers
    res = []
    for prov in providers:
        prov_id= prov
        info = r.hgetall('providers:' + prov_id)
        res.append({'id': prov_id, 'name': info['name'], 'city': info['city'], 'state': info['state'], 'score': info['avg_overcharge']})

    return render_template('list.html', providers=res)

@app.route("/about", methods=['GET'])
def about():
    return render_template('about.html')

@app.route("/search", methods=['POST'])
def search():
    key = ""
    #print request.form
    if 'state' in request.form and request.form['state'] != "":
        key = key + "state:" + request.form['state']
        union = False
        if 'zip' in request.form and request.form['zip'] != "":
            union = True
            new_key = "zip:" + request.form['zip']
            union_key = key + ":" + new_key
            if not r.exists(union_key):
                r.zinterstore(union_key, {key:0, new_key:1})
            key = union_key
            r.expire(key, 6000)

        if 'drg' in request.form and request.form['drg'] != "":
            union = True
            new_key = "drg:" + request.form['drg'] + ":providers"
            union_key = key + ":" + new_key
            if not r.exists(union_key):
                r.zinterstore(union_key, {key:0, new_key:1})
            key = union_key
            r.expire(key, 6000)

        if not union:
            key = "state:" + request.form['state']

    elif 'zip' in request.form and request.form['zip'] != "":
        key = key + "zip:" + request.form['zip']
        union = False

        if 'drg' in request.form and request.form['drg'] != "":
            union = True
            new_key = "drg:" + request.form['drg'] + ":providers"
            union_key = key + ":" + new_key
            if not r.exists(union_key):
                r.zinterstore(union_key, {key:0, new_key:1})
            key = union_key
            r.expire(key, 6000)

        if not union:
            key = "zip:" + request.form['zip']

    elif 'drg' in request.form and request.form['drg'] != "":
        key = "drg:" + request.form['drg'] + ":providers"

    else:
        key = 'providers'

    #print "key is " + key

    try:
        desc = request.form['reverse']
    except KeyError, e:
        desc = False

    providers = r.zrange(key, 0, 100, withscores=True, desc=desc)
    res = []
    for prov in providers:
        prov_id, score = prov
        info = r.hgetall('providers:' + prov_id)
        res.append({'id': prov_id, 'name': info['name'], 'city': info['city'], 'state': info['state'], 'score': score})

    return render_template('list.html', providers=res)

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
