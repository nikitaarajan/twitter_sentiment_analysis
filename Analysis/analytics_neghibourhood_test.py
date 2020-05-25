#This program filter out tweets with neighbourhood bounding box
#to plot on Google Map
import couchdb
from textblob import TextBlob

#connect to local server
localserver = couchdb.Server("http://admin:admin@172.26.133.251:5984/")
dbname = "neighbourhood"
if dbname in localserver:
        del localserver[dbname]
local = localserver.create(dbname)

#connect to tweet DB
crawler = localserver['tweettest']

for i in crawler:
        try:
                tweet = crawler[i]["doc"]
                place_type = tweet["place"]["place_type"]
                if place_type == 'neighborhood':
                        coordinates = tweet["coordinates"]["coordinates"]
                        place_name = tweet["place"]["name"]
                        timestampe = tweet["created_at"]
                        day = tweet["created_at"][:3]
                        time = tweet["created_at"][11:13]
                        text = tweet["text"]
                        length = len(text)
                        box = tweet["place"]["bounding_box"]['coordinates']
                        sentiment = TextBlob(text).polarity
                        userid = tweet["user"]["id_str"]
                        hashtags = tweet['entities']['hashtags']
                        user_location = tweet['user']['location']
                        doc = {
                                'place_name' : place_name,
                                'timestampe' : timestampe,
                                'day' : day,
                                'time': time,
                                'text' : text,
                                'length': length,
                                'bounding_box': box,
                                'sentiment' : sentiment,
                                'coordinates' : coordinates,
                                'userid' : userid,
                                'hashtags': hashtags,
                                'user_location': user_location
                        }
                        local.save(doc)
        except Exception as e:
                print("A possible exception was raised")
                pass