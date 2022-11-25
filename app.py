from fastapi import FastAPI
from decouple import config
from pytrends.request import TrendReq
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

@app.get("/youtube_get_region")
async def youtubeGetRegion():
    return pd.read_csv('google_region.csv')['region'].to_list()

@app.get("/youtube_get_top_query")
async def youtubeGetTopQuery(query:str,region:str,nrows:int):
    if(query is None):
        return {'error':'query param is null!'}
    else:
        locationsDF = pd.read_csv('google_region.csv')
        locationsDF['region'] = locationsDF['region'].str.lower()
        lokasi = locationsDF[locationsDF['region'] == region]['code'].to_list()
        if(len(lokasi)>0 and region.isalpha()):
            lokasi = locationsDF[locationsDF['region'] == region]['code'].to_list()[0]
            pytrends = TrendReq(hl='en-US', tz=360)
            kw_list = [query]
            pytrends.build_payload(kw_list, cat=0, timeframe='today 5-y', geo=lokasi, gprop='youtube')
            df_queries = pytrends.related_queries()
            df_top = df_queries.get(query).get("top")
            if(df_top is not None):
                df_top = df_top.sort_values(['value'], ascending=False).head(nrows).reset_index(drop=True)
                json_list = json.loads(json.dumps(list(df_top.T.to_dict().values())))
                return json_list
            else:
                return {'error':'query returned no data!'}
        else:
            return {'error':'region does not exist!'}

if __name__ == '__main__':
    uvicorn.run("app:app", host='0.0.0.0', port=5781, log_level="info", reload=True)