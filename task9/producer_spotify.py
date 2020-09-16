from secrets import *
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
from kafka import KafkaProducer
import time

cid = client_id
secret = client_secret

client_credentials_manager = SpotifyClientCredentials(client_id=cid,client_secret = secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

#Setup kafka producer
producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],value_serializer=lambda x:json.dumps(x).encode('utf-8'))

for i in range(0,200,50):
    track_results = sp.search(q='year:2018', type='track', limit=50,offset=i)
    
    for i, t in enumerate(track_results['tracks']['items']):
        #send to kafka
	print(i)
	producer.send('spotify_tracks',t)
	producer.flush()
	time.sleep(10)	
