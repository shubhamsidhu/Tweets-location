from geopy import OpenMapQuest
import keys
from textblob import TextBlob
import time
import tweepy
import pandas as pd
import folium
import sys


# Function for connecting to tweet api

def get_API(wait=True, notify=True):

    auth = tweepy.OAuthHandler(keys.consumer_key, keys.consumer_secret)
    auth.set_access_token(keys.access_token, keys.access_token_secret)

    return tweepy.API(auth, wait_on_rate_limit=wait,
                      wait_on_rate_limit_notify=notify)

#function for getting the tweets 
def get_tweet_content(tweet, location=False):
    """Return dictionary with data from tweet (a Status object)."""
    fields = {}
    fields['screen_name'] = tweet.user.screen_name

    # get the tweet's text
    try:  
        fields['text'] = tweet.extended_tweet.full_text
    except: 
        fields['text'] = tweet.text

    if location:
        fields['location'] = tweet.user.location

    return fields


class LocationListener(tweepy.StreamListener):
    """Handles incoming Tweet stream to get location data."""

    def __init__(self, api, counts_dict, tweets_list, topic, limit=10):
        """Configure the LocationListener."""
        self.tweets_list = tweets_list
        self.counts_dict = counts_dict
        self.topic = topic
        self.TWEET_LIMIT = limit
        super().__init__(api)  # call superclass's init

    def on_status(self, status):
        """Called when Twitter pushes a new tweet to you."""
        # get each tweet's screen_name, text and location
        tweet_data = get_tweet_content(status, location=True)  

        # ignore retweets and tweets that do not contain the topic
        if (tweet_data['text'].startswith('RT') or
            self.topic.lower() not in tweet_data['text'].lower()):
            return

        self.counts_dict['total_tweets'] += 1  # original tweet

        # ignore tweets with no location 
        if not status.user.location:  
            return

        self.counts_dict['locations'] += 1  # tweet with location
        self.tweets_list.append(tweet_data)  # store the tweet
        print(f'{status.user.screen_name}: {tweet_data["text"]}\n')

        # if TWEET_LIMIT is reached, return False to terminate streaming
        return self.counts_dict['locations'] < self.TWEET_LIMIT

# Function to get the cooridinates of place of the tweets
def get_geocodes(tweet_list):
    """Get the latitude and longitude for each tweet's location.
    Returns the number of tweets with invalid location data."""
    print('Getting coordinates for tweet locations...')
    geo = OpenMapQuest(api_key=keys.mapquest_key)  # geocoder
    bad_locations = 0  

    for tweet in tweet_list:
        processed = False
        delay = .1  # used if OpenMapQuest times out to delay next call
        while not processed:
            try:  # get coordinates for tweet['location']
                geo_location = geo.geocode(tweet['location'])
                processed = True
            except:  # timed out, so wait before trying again
                print('OpenMapQuest service timed out. Waiting.')
                time.sleep(delay)
                delay += .1

        if geo_location:  
            tweet['latitude'] = geo_location.latitude
            tweet['longitude'] = geo_location.longitude
        else:  
            bad_locations += 1  # tweet['location'] was invalid
    
    print('Done geocoding')
    return bad_locations


def main():

	topic = sys.argv[1]
	limit = int(sys.argv[2])
	api = get_API()
	tweets = [] #list to store the tweets from stream 
	counts = {'total_tweets': 0 ,'locations': 0 }

	location_listener = LocationListener(api,counts_dict= counts,tweets_list = tweets , topic = topic , limit = limit)
	stream = tweepy.Stream(auth = api.auth, listener = location_listener)
	stream.filter(track = [topic] ,languages = ['en'], is_async= False)
	

	stat_of_count = f"{counts['locations']/counts['total_tweets']}"


	bad_locations = get_geocodes(tweets)
	
	data_frame = pd.DataFrame(tweets)
	data_frame = data_frame.dropna()
	

	IE_map = folium.Map(location = [53.305494, -7.737649], tiles = 'Stamen Terrain',
						zoom_start = 4 , detect_retina = True)
	
	# to make the tweets on the location 
	for t in data_frame.itertuples():
		text = ':'.join([t.screen_name, t.text])
		popup = folium.Popup(text, parse_html = True)
		marker = folium.Marker((t.latitude,t.longitude), popup = popup)
		marker.add_to(IE_map)
		
	IE_map.save(f'{topic}.html')

if __name__ == '__main__':

	main()
		


