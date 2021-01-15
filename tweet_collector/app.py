from datetime import datetime
import config
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import json
import pymongo
import requests
from urllib3.exceptions import ProtocolError

# connect to local MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client.usElectionsTweets


def authenticate():
    """Function for handling Twitter Authentication. Please note
       that this script assumes you have a file called config.py
       which stores the 4 required authentication tokens:

       1. CONSUMER_API_KEY
       2. CONSUMER_API_SECRET
       3. ACCESS_TOKEN
       4. ACCESS_TOKEN_SECRET

    See course material for instructions on getting your own Twitter credentials.
    """
    auth = OAuthHandler(config.CONSUMER_API_KEY, config.CONSUMER_API_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

    return auth

def getLocationOfCity(city):
    api_key = config.OPEN_CAGE_API_KEY
    req = requests.get('https://api.opencagedata.com/geocode/v1/json?q='+city+'&key='+api_key)
    js = json.loads(req.text)
    rep = None
    for res in js['results']:
        if res['formatted'].split(',')[-1] == " United States of America":
            rep = res['geometry'] 
            break
    return rep

class TwitterListener(StreamListener):
	def on_data(self, data):
		"""
			Whatever we put in this method defines what is done with
			every single tweet as it is intercepted in real-time
		"""
		try:
			t = json.loads(data)  # t is just a regular python dictionary.
			text = t["text"]
			if "extended_tweet" in t:
				text = t["extended_tweet"]["full_text"]
			if "retweeted_status" in t:
				r = t["retweeted_status"]
				if "extended_tweet" in r:
					text = r["extended_tweet"]["full_text"]
			keyword = None
			for person in [
				"Trump",
				"Biden"
				]:
				if (person in text) or (person in t["entities"]["hashtags"]):
					keyword = person
			location = None
			if t["user"]["location"] is not None:
				location = getLocationOfCity(t["user"]["location"])

			if location is not None:
				tweet = {
					"text": text,
					"username": t["user"]["screen_name"],
					"followers_count": t["user"]["followers_count"],
					"timestamp": datetime.strptime(t["created_at"], "%a %b %d %H:%M:%S +0000 %Y"),
					"lng": location["lng"],
					"lat": location["lat"],
					"keyword": keyword,
				}

				# write to mongo collection 'tweets'
				db.usElectionsTweets.insert(tweet)
				# print(tweet)
		except Exception as e:
			print(e)

	def on_error(self, status):
		if status == 420:
			print(status)
			return False


if __name__ == "__main__":
    auth = authenticate()
    listener = TwitterListener()
    stream = Stream(auth, listener)
    while True:
    	try:
    		stream.filter(
				track=["Trump","Biden"],
				languages=["en"],
				stall_warnings=True
			)
    	except ProtocolError:
    		continue