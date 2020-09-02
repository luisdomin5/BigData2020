import requests

locations = {'london':('51.509865','-0.118092'),'bristol':('51.454514','2.587910'),'new york':('40.730610','-73.935242')}

url = "https://climacell-microweather-v1.p.rapidapi.com/weather/"

fields_hourly = ','.join(['temp','precipitation','humidity','wind_speed','wind_direction','precipitation','precipitation_type','precipitation_probability','weather_code'])
fields_realtime = ','.join(['temp','precipitation','humidity','wind_speed','wind_direction','precipitation','precipitation_type','weather_code'])

#querystring = {'fields':','.join(fields),"unit_system":"si","lat":"42.8237618","lon":"-71.2216286"}

with open('api-key') as f:
       api_key = f.read().strip()

headers = {
    'x-rapidapi-host': "climacell-microweather-v1.p.rapidapi.com",
    'x-rapidapi-key': api_key
    }

#response = requests.request("GET", url_realtime, headers=headers, params=querystring)

def get_weather_data(type,location,header=headers,url=url):
	if type=='hourly':
		url+='forecast/hourly'
		fields = fields_hourly
	elif type=='realtime':
		url+='realtime'
		fields = fields_realtime
	else:
		print('type not supported: ',type)
		return
	lat,long = locations[location][0],locations[location][1]
	query = {'fields':fields,'lat':lat,'lon':long,'unit_system':'si'}
	response = requests.request('GET',url,headers=header,params=query)
	return response
