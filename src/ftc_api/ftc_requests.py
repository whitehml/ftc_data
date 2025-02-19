import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import os

### Helpers
def pickle_df_to_folder(file_path, df):
	"""Pickles a DataFrame, creating the folder if it doesn't exist."""
	directory = os.path.dirname(file_path)
	os.makedirs(directory, exist_ok=True)
	df.to_pickle(file_path)

def change_authorization():
	user = input("Enter new username: ")
	api_key = input("Enter new API key: ")
	with open("src/ftc_api/.auth", "w") as auth_file:
		auth_file.write(user)
	with open("src/ftc_api/.api-key", "w") as api_file:
		api_file.write(api_key)

class FtcRequests:
	"""
	Connects the FTC Event API to a local database.

	Requires user to use their own API key.
	"""

	# static values
	SERVER = "https://ftc-api.firstinspires.org/"
	FILLER = [
		'eventId','remote','webcasts','timezone','coordinates', 'hybrid','country','website','liveStreamUrl','leagueCode',
		'districtCode','venue','divisionCode','address','dateEnd','fieldCount','published','type','stateprov','regionCode',
		'description', 'modifiedOn', 'actualStartTime', 'scoreRedFinal', 'scoreRedFoul', 'scoreRedAuto', 'scoreBlueFinal', 
		'scoreBlueFoul', 'scoreBlueAuto'
		]
	# script_dir = os.path.dirname(os.path.abspath(__file__))



	def __init__(self):
		if not os.path.exists("src/ftc_api/.auth") and not os.path.exists("src/ftc_api/.api-key"):
			change_authorization()

		with open("src/ftc_api/.api-key", 'r', encoding='utf-8-sig') as api_file:
			self.API_KEY = api_file.read().strip()
		with open("src/ftc_api/.auth", 'r', encoding='utf-8-sig') as auth_file:
			self.USER = auth_file.read().strip()
		self.HEADERS = {'Accept': 'application/json', 'Authorization': self.API_KEY}

	def get_region_events(self, region='USPA', year=2024):
		"""
		Populates databate with competetiion event information in a given region and year.

		Args:
			region: 4 character region code e.g. 'USPA'
			year: year of comeptetion kickoff
		"""
		query = requests.get(self.SERVER + "v2.0/{}/events".format(year), auth=HTTPBasicAuth(self.USER, self.API_KEY), headers=self.HEADERS)
		ef = pd.DataFrame(query.json()['events'])
		ef = ef[ef.code.str[:4] == region]
		ef = ef[ef.typeName.isin(['Scrimmage', 'Qualifier', 'Championship'])]
		ef = ef.drop(columns=ef.columns.intersection(self.FILLER),axis=1)
		ef = ef.sort_values(by='dateStart').reset_index(drop=True).rename(columns={'typeName':'type', 'dateStart':'date'})
		pickle_df_to_folder('data/{}/events.pkl'.format(year),ef)
		return ef

	def get_event_teams(self, event_code, year=2024):
		"""
		Returns a dict of team IDs to team names that are participating in the event.

		Args:
			event_code: alphanumeric event identifier string e.g. 'USPAPHQ1'
			year: year of comeptetion kickoff
		"""
		params = {"eventCode" : event_code}
		query = requests.get(self.SERVER + "v2.0/{}/teams".format(year), auth=HTTPBasicAuth(self.USER, self.API_KEY), headers=self.HEADERS, params=params)
		breakpoint()
		tf = pd.DataFrame(query.json()['teams'])
		return dict(zip(tf.teamNumber, tf.nameShort))
	
	def get_event_data(self, event_code, year=2024):
		"""
		Returns event qualifier, match, and playoff dataframes.

		Args:
			event_code: alphanumeric event identifier string e.g. 'USPAPHQ1'
			year: year of comeptetion kickoff
		"""
		query = requests.get(self.SERVER + "v2.0/{}/matches/{}".format(year,event_code), auth=HTTPBasicAuth(self.USER, self.API_KEY), headers=self.HEADERS)
		mf = pd.DataFrame(query.json()['matches'])
		mf = mf.drop(columns=mf.columns.intersection(self.FILLER),axis=1)
		query = requests.get(self.SERVER + "v2.0/{}/scores/{}/qual".format(year,event_code), auth=HTTPBasicAuth(self.USER, self.API_KEY), headers=self.HEADERS)
		qf = pd.DataFrame(query.json()['matchScores'])
		qf = qf.drop(columns=qf.columns.intersection(['randomization']))
		query = requests.get(self.SERVER + "v2.0/{}/scores/{}/playoff".format(year,event_code), auth=HTTPBasicAuth(self.USER, self.API_KEY), headers=self.HEADERS)
		pf = pd.DataFrame(query.json()['matchScores'])
		pf = pf.drop(columns=pf.columns.intersection(['randomization']))
		return {'mf':mf, 'qf':qf, 'pf':pf}
	
	def get_events_data(self, event_codes, year=2024):
		"""
		Returns a list of event qualifier, match, and playoff dataframes.

		Args:
			event_code: list of alphanumeric event identifier strings e.g. ['USPAPHQ1', 'USPAPHQ2']
			year: year of comeptetion kickoff
		"""
		match_data = []
		for code in event_codes:
			match_data.append(self.get_event_data(code, year))
		return match_data