import tweepy
import couchdb
import json
import math
from urllib3.exceptions import ProtocolError
# Twitter API authentication details.
consumer_key = 'ojOqMVHEPmhCiYwTrZIUrPGGW'
consumer_secret = 'LJjqy5MNmoN9iKPAzODyD6C4r4WlzEBG6BbDaFMfQCVorAdT6L'
access_token = '1258052304462176258-YLPE8lyYuVnBYDif83I3QCinqwGJ0c'
access_token_secret = 'Ix7ShXKNWPkEYIaab42KNAybsnepWgvEtY4wnBdqamTSA'


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

db_tweet_name = 'tweet'
db_user_name = 'user'
db_address = "http://admin:admin@172.26.133.251:5984/"
db_server = couchdb.Server(db_address)

if db_tweet_name in db_server:
    db_tweet = db_server[db_tweet_name]
else:
    db_tweet = db_server.create(db_tweet_name)

if db_user_name in db_server:
    db_user = db_server[db_user_name]
else:
    db_user = db_server.create(db_user_name)

bounding_box = [113.338953078, -43.6345972634, 153.569469029, -10.6681857235]

num_harvester = 2

harvester_code = 0

def create_sub_bbox(bounding_box, num):
    sub_bbox = []
    interval = math.floor((bounding_box[2] - bounding_box[0])/num*100000000)/100000000
    for i in range(num):
        sub_bbox.append([bounding_box[0]+interval*i, bounding_box[1], bounding_box[0]+interval*(i+1), bounding_box[3]])

    sub_bbox[num-1][2]=bounding_box[2]

    return sub_bbox

sub_bbox = create_sub_bbox([113.338953078, -43.6345972634, 153.569469029, -10.6681857235], num_harvester)

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        print(status.text)

    def on_data(self, data):
        tweet = json.loads(data)
        doc_id = tweet["id_str"]
        if doc_id not in db_tweet:
            db_tweet[doc_id] = {"tweet": tweet}
        print("new tweet: "+doc_id)
        if tweet["user"]["id_str"] not in db_user:
            try:
                for status in tweepy.Cursor(api.user_timeline, screen_name=tweet["user"]["screen_name"], tweet_mode='extended').items():
                    user_tweet = status._json
                    if user_tweet["coordinates"] is not None:
                        if sub_bbox[harvester_code][2] > user_tweet["coordinates"]["coordinates"][0] > sub_bbox[harvester_code][0] \
                                and sub_bbox[harvester_code][3] > user_tweet["coordinates"]["coordinates"][1] > sub_bbox[harvester_code][1]:
                            doc_id = user_tweet["id_str"]
                            if doc_id not in db_tweet:
                                db_tweet[doc_id] = {"tweet": tweet}
                                print("new tweet: "+doc_id)
                db_user[tweet["user"]["id_str"]] = {"complete": "y"}
                print("user: "+tweet["user"]["id_str"]+" completed")
            except Exception as e:
                print(e)
                pass
        return True

    def on_error(self, status):
        print(status)

    def on_exception(self, exception):
        print(exception)
        return


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

while True:
    try:
        myStream.filter(locations=sub_bbox[harvester_code])
    except ProtocolError:
        continue
