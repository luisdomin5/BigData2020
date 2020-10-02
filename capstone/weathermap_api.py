import requests
import json
import pandas as pd
base_url = "http://api.openweathermap.org/data/2.5/weather?"
locations = pd.read_csv('locations.csv',index_col='name')
with open("api_key") as f:
    api_key = f.read().strip()

def weather_lat_lon(lat,lon,base_url=base_url,api_key=api_key):
	q = base_url+"lat={}&lon={}&units=metric&appid={}".format(lat,lon,api_key)
	res = requests.get(q)
	return res


def current_location(loc):
	lat,lon = get_loc(loc)
	return weather_lat_lon(lat,lon)


def forecast(loc):
	lat,lon = get_loc(loc)
	return weather_lat_lon(lat,lon,base_url="http://api.openweathermap.org/data/2.5/forecast?")

def get_loc(loc):
	loc=loc.lower()
	if(loc in locations.index):
		return list(locations.xs(loc))
