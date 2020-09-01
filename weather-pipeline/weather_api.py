import requests

# url for requests
url = "https://community-open-weather-map.p.rapidapi.com/weather"

# Get api key from text file, keep hidden from github
with open('api-key') as f:
	api_key = f.read().strip()

headers =  {'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
		'x-rapidapi-key':api_key}


def get_current_weather(location,header=headers,url=url):
	query = {'q':location}
	response = requests.request('GET',url,headers=header,params=query)
	return response

	
	
