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

trendDF = None

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

@app.get("/youtube_top_query")
def youtubeGetTopQuery():
    return getTrendingQuery()

def getTrendingQuery():
    pytrends = TrendReq(hl='en-US', tz=360)
    return pytrends.trending_searches(pn='indonesia')[0].to_list()


@app.get("/youtube_trending")
async def youtubeGetTopInterest():
    trendingQuery = getTrendingQuery()
    if(trendingQuery is None or len(trendingQuery) < 1):
        return {'error':'no data was returned!'}
    else:
        dfHasil = None
        for kuerinya in trendingQuery:
            kw_list = [kuerinya]
            pytrends = TrendReq(hl='en-US', tz=360)
            pytrends.build_payload(kw_list, cat=0, timeframe='all', geo='ID', gprop='')
            a = pytrends.interest_by_region(resolution='CITY', inc_low_vol=True, inc_geo_code=False)
            b = a.T
            if(dfHasil is None):
                dfHasil = b.copy(deep=True)
            else:
                dfHasil = pd.concat([dfHasil,b])
        daftarHasil = list()
        json_list = json.loads(json.dumps(list(dfHasil.T.to_dict().values())))
        jsonIndex = 0
        for index, row in dfHasil.iterrows():
            hasilLokal = dict()
            hasilLokal['query'] = index
            hasilLokal['summary'] = json_list[jsonIndex]
            jsonIndex += 1
            daftarHasil.append(hasilLokal)
        return json.loads(json.dumps(daftarHasil))

if __name__ == '__main__':
    uvicorn.run("app:app", host='0.0.0.0', port=5781, log_level="info", reload=True)