import json
import tweepy
from tweepy import OAuthHandler
import time
import couchdb
import os

# Twitter API authentication details.
consumer_key = 'ojOqMVHEPmhCiYwTrZIUrPGGW'
consumer_secret = 'LJjqy5MNmoN9iKPAzODyD6C4r4WlzEBG6BbDaFMfQCVorAdT6L'
access_token = '1258052304462176258-YLPE8lyYuVnBYDif83I3QCinqwGJ0c'
access_secret = 'Ix7ShXKNWPkEYIaab42KNAybsnepWgvEtY4wnBdqamTSA'

member = 'admin'
pw = 'admin'
host = '172.26.130.74'
port = 5984
debug = True

server = 'http://'+ member +':'+ pw+'@'+host+':'+str(port)+'/'
# CouchDB authentication details.
couch = couchdb.Server(server)
tweetdb = couch.create('tweets1')

class StdOutListener(tweepy.StreamListener):
    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        decoded = json.loads(data)
        try:
            # set the doc id
            doc = {
                '_id':decoded['id_str']
            }
            # update the rest of the json object into doc
            doc.update(decoded)
            #print(doc)
            os.system("du -csh {}".format(tweetdb.save(doc)))
            # put the doc in the db
            print(tweetdb.save(doc) + '\n')
        except Exception as e:
            print(e)
            # print json.dumps(decoded,indent=4)
            print()
        return

l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
while True:
    try:
        stream = tweepy.Stream(auth, l)
        stream.filter(locations=[144.5937,-38.59,145.5125,-37.5113])
    except:
        print('sleeping for 15 mins\n')
        time.sleep(60 * 15)
        continue