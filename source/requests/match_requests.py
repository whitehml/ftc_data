import requests
from requests.auth import HTTPBasicAuth
import pandas as pd

import sys
sys.path.insert(1, '../source/stats')
from stats import process_event

SERVER = "https://ftc-api.firstinspires.org/"
with open("../source/requests/.api-key", 'r', encoding='utf-8-sig') as api_file:
	API_KEY = api_file.read().strip()

with open("../source/requests/.auth", 'r', encoding='utf-8-sig') as api_file:
	USER = api_file.read().strip()

HEADERS = {'Accept': 'application/json', 'Authorization': API_KEY}

events = pd.read_pickle('../../data/events.pkl')

### Find all matches for a qual
dfs = []
for eventCode in scrims:
	query = requests.get(SERVER + "v2.0/2024/matches/{}".format(eventCode), auth=HTTPBasicAuth(USER, API_KEY), headers=HEADERS)
	mf = pd.DataFrame(query.json()['matches'])
	query = requests.get(SERVER + "v2.0/2024/scores/{}/qual".format(eventCode), auth=HTTPBasicAuth(USER, API_KEY), headers=HEADERS)
	qf = pd.DataFrame(query.json()['matchScores'])
	query = requests.get(SERVER + "v2.0/2024/scores/{}/playoff".format(eventCode), auth=HTTPBasicAuth(USER, API_KEY), headers=HEADERS)
	pf = pd.DataFrame(query.json()['matchScores'])
	dfs.append(process_event(mf, qf, pf, eventCode))