import couchdb
import sys, json
from argparse import ArgumentParser
from io import open

parser = ArgumentParser(description='Processes a JSON tweet file and store in a CouchDB Database')
parser.add_argument('--dbname', help='CouchDB database name', required=True, type=str)
parser.add_argument('--debug', help='Print debug messages to screen', action='store_true',default=True)
parser.add_argument('--bulk', help='Bulk update number (30000)', type=int, default=500)
args = parser.parse_args()

member = 'admin'
pw = 'admin'
host = '172.26.133.251'
port = 5984
debug = True

server = 'http://'+ member +':'+ pw+'@'+host+':'+str(port)+'/'
# CouchDB authentication details.
couch = couchdb.Server(server)
db = couch.create(args.dbname)
total = 0
docs = []
file = '../untitled1/tinyTwitter.json'
with open(file, encoding="utf-8") as f:
    for line in f:
        try:
            lo = line.find('{')
            hi = line.rfind('}')
            print(lo, hi)
            if(lo !=-1 and hi != -1):
                tweet = json.loads(line[lo:hi+1])
                print("tweet", tweet)
                doc = {}
                doc.update(tweet)
                doc['_id'] = tweet['id']
                if args.debug is True:
                    print(json.dumps(doc, indent=4))
                # add doc to the list of docs
                docs.append(doc)
                if len(docs) % args.bulk == 0:
                    db.update(docs)
                    total += len(docs)
                    print('updated: ' + str(len(docs)) + ' total: ' + str(total))
                    docs = []
        except:
            pass
    if len(docs) > 0:
        db.update(docs)
        total += len(docs)
        print('updated: ' + str(len(docs)) + ' total: ' + str(total))