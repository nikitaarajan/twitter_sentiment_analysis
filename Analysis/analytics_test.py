# This program filter raw tweet medadata into a cleaner format
# to reduce storage space required and processing time.
import couchdb
from textblob import TextBlob
import re

# connect to local server
couchserver = couchdb.Server("http://admin:admin@172.26.133.251:5984/")
# delete database if exist
dbname = "clean_data_kaustub"
if dbname in couchserver:
    del couchserver[dbname]
clean = couchserver.create(dbname)

# connect to raw tweet databse
crawler = couchserver['tweettest']
# filter tweets to a cleaner format
def remove_non_letters(text):
    # regex patterns
    hashtag = r'#\S+'
    email = r'[\w\d\._-]+@\w+(\.\w+){1,3}'
    website = r'http\S+|www\.\w+(\.\w+){1,3}'
    retweet = r'RT\s@\S+'
    mention = r'@[\w\d]+'
    punctual = r'[_\+-\.,!@\?#$%^&*();\\/|<>"\':]+'
    weird = r'�+'
    newline = r'\n'
    spaces = r'\s{2,}'
    digits = r'\d+'

    combined_patterns = r'|'.join((hashtag, email, website, retweet, mention, punctual, weird, newline, digits))
    stripped = re.sub(combined_patterns, ' ', text)
    # remove extra whitespaces
    stripped = re.sub(spaces, ' ', stripped)
    stripped = stripped.strip()
    return stripped

def remove_emojis(text):
        emoji_pattern = re.compile(
        u"(\ud83d[\ude00-\ude4f])|"  # emoticonsa
        u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
        u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
        u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
        u"(\ud83c[\udde0-\uddff])|"  # flags (iOS)
        "\+", flags=re.UNICODE)
        # return ''.join(c for c in str if c not in emoji.UNICODE_EMOJI)
        return emoji_pattern.sub (r' ', text)

def filter_tweet(text):
    text = remove_emojis(text)
    text = remove_non_letters(text)
    if not text or len(text) == 0:
        return
    else:
        return text


for i in crawler:
    try:
        tweet = crawler[i]["doc"]
        print("hello", tweet)
        if ('en' in tweet['lang']):  # if tweet in English
            id = tweet["id"]
            day = tweet["created_at"][:3]
            time = tweet["created_at"][11:13]
            location = tweet['user']['location']
            hashtags = tweet['entities']['hashtags']
            lang =  tweet['lang']
            place = tweet['place']
            favorite_count = tweet['favorite_count']
            source = tweet['source'][37:44]
            rt = tweet["retweet_count"]
            coordinates = tweet["coordinates"]["coordinates"]
            text = filter_tweet(tweet['text'])
            length = len(text)
            bbox = tweet["place"]["bounding_box"]['coordinates']
            polarity = TextBlob(text).sentiment.polarity
            subjectivity =  TextBlob(text).sentiment.subjectivity

            doc = {
                'id': id,
                'day': day,
                'hour': time,
                'user_location': location,
                'hashtags': hashtags,
                'lang': lang,
                'place':place,
                'favorite_count':favorite_count,
                'source': source,
                'rt': rt,
                'text': text,
                'length': length,
                'bounding_box': bbox,
                'polarity': polarity,
                'subjectivity': subjectivity,
                'coordinates': coordinates
            }
            # save document to two database for backup
            clean.save(doc)
    except Exception as e:
        print("Hi there")
        pass


