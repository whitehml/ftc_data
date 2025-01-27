import requests
from requests.auth import HTTPBasicAuth

SERVER = "https://ftc-api.firstinspires.org/"
with open(".api-key", 'r', encoding='utf-8-sig') as api_file:
	API_KEY = api_file.read().strip()

with open(".auth", 'r', encoding='utf-8-sig') as api_file:
	USER = api_file.read().strip()

HEADERS = {'Accept': 'application/json', 'Authorization': API_KEY}
AUTH = (USER, API_KEY)

query = requests.get(SERVER + "v2.0/2024/leagues?regionCode=USPA", auth=HTTPBasicAuth(USER, API_KEY), headers=HEADERS)
query = requests.get(SERVER + "v2.0/2024/events?eventCode=USPALAQ2", auth=HTTPBasicAuth(USER, API_KEY), headers=HEADERS)

import pandas as pd
df = pd.DataFrame(query.json()['events'])
df = df[df['regionCode'] == 'USPA']
df = df[(df['typeName'] == 'Scrimmage') | (df['typeName'] == 'Qualifier')]

df = df.drop(labels=['divisionCode'],axis=1)
df = df.drop(labels=['eventId','remote','webcasts','timezone','coordinates'],axis=1)
df = df.drop(labels=['hybrid','country','website','liveStreamUrl'],axis=1)
df = df.drop(labels=['leagueCode','districtCode','venue'],axis=1)

### Find all matches for a qual
query = requests.get(SERVER + "v2.0/2024/matches/USPALAQ2", auth=HTTPBasicAuth(USER, API_KEY), headers=HEADERS)
mf = pd.DataFrame(query.json()['matches'])

query = requests.get(SERVER + "v2.0/2024/scores/USPALAQ2/qual", auth=HTTPBasicAuth(USER, API_KEY), headers=HEADERS)
qf = pd.DataFrame(query.json()['matchScores'])

query = requests.get(SERVER + "v2.0/2024/scores/USPALAQ2/playoff", auth=HTTPBasicAuth(USER, API_KEY), headers=HEADERS)
pf = pd.DataFrame(query.json()['matchScores'])