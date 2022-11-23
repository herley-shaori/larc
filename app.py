from fastapi import FastAPI
from decouple import config
import uvicorn
import json
import pandas as pd
import requests

app = FastAPI()

json_file_path = config('WOEID_PATH')
with open(json_file_path, 'r') as j:
    contents = json.loads(j.read())
df = pd.DataFrame(contents)
df.drop(columns=['placeType'], inplace=True)
df['name'] = df['name'].str.lower()
df['country'] = df['country'].str.lower()

@app.get("/")
async def root():
    return {"message": "The program is working."}

@app.get("/twitter_get_city")
async def getAllCity():
    return df['name'].to_list()

@app.get("/twitter_get_city_per_country")
async def getCityInACountry(country:str):
    if (df['country'].str.contains(country.lower()).any()):
        dfCityInACountry = df[df['country'] == country]
        return dfCityInACountry['name'].to_list()
    else:
        return {'error':'country not found. please see the supported country list!'}

@app.get("/twitter_trends_per_city")
async def getCityPerCountryTrends(country:str,city:str):
    if (df['country'].str.contains(country.lower()).any()):
        if (df['name'].str.contains(city.lower()).any()):
            dfCityInACountry = df[(df['country'] == country) & (df['name'] == city)]
            woeid = dfCityInACountry['woeid'].to_list()[0]
            url = "https://api.twitter.com/1.1/trends/place.json?id=%s"%(str(woeid))

            payload = {}
            headers = {
                'Authorization': 'Bearer %s'%(config('BEARER'))
            }
            response = requests.request("GET", url, headers=headers, data=payload)
            return json.loads(response.text)
        else:
            return {'error': 'location not found!'}
    else:
        return {'error': 'country not found. please see the supported country list!'}

if __name__ == '__main__':
    uvicorn.run("app:app", host='0.0.0.0', port=5781, log_level="info", reload=True)