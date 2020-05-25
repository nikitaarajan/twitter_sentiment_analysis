import couchdb
import json
from collections import defaultdict
import time
import datetime

VIEWS = ['avgsentiment','daysum','hoursum','locsum','lensum','hashtagsum']

while True:
    couch = couchdb.Server("http://admin:admin@172.26.133.251:5984")
    d = defaultdict()
    try:
            print(datetime.datetime.now(),"retrieving results from couchdb views")
            db = couch["tweet_db_clean"]

            for name in VIEWS:
                    i = 0
                    viewname = "box/"+name

                    filename = name+".json"

                    viewresult = db.view(viewname,group=True)

                    for item in viewresult:
                        d[i] = defaultdict()
                        d[i]["key"] = item.key
                        d[i]["value"] = item.value
                        print("printing values",d[i]["value"])
                        i+=1
                    d['count'] = i
                    with open(filename, "w+") as f:
                            json.dump(d,f)
                    d.clear()
            time.sleep(300)
    except Exception as ex:
            print("An exception heppened when pulling data from couchdb:",str(ex))
            pass

