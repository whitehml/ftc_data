import pandas as pd
import numpy as np
from src.ftc_api.ftc_requests import FtcRequests

# Combine scoring methods into stats
def calc_category_stats(stats, agg_stats):
	stats['Bucket'] = stats['Bucket'] = 2 * (stats.aNet_O + stats.tNet_O) + 4 * (stats.aSMPL_O + stats.tSMPL_O) + 8 * (stats.aSMPH_O + stats.tSMPH_O)
	stats['Specimen'] = 5 * (stats.aSPCL_O + stats.tSPCL_O) + 10 * (stats.aSPCH_O + stats.tSPCH_O) 
	ascent = []
	stdDev = []
	for i, r in stats.iterrows():
		agg_row = agg_stats.loc[(r.eventCode, r.teamNumber, False)]
		ascent.append((agg_row.location + agg_row.ascent) / agg_row.matchNumber)
		stdDev.append(agg_row.stdDev)
	stats['End of Round'] = ascent
	stats['stdDev'] = stdDev
	stats['Foul'] = -5 * stats.miFoul_O + -15 * stats.maFoul_O

def calc_auto_oprs(stats):
	stats['AutoBucket'] = 2 * stats.aNet_O + 4 * stats.aSMPL_O+ 8 * stats.aSMPH_O
	stats['AutoSpecimen'] = 5 * stats.aSPCL_O + 10 * stats.aSPCH_O

def calc_adj_categories(stats):
	stats['Bucket+'] = stats.Bucket + stats.Foul + stats['End of Round']
	stats['Specimen+'] = stats.Specimen + stats.Foul + stats['End of Round']

# Classify teams
def select_qualifiers(df):
	return df[df.eventCode.str[-1:] != 'S']

def select_scrimmages(df):
	return df[df.eventCode.str[-1:] == 'S']

def classify_teams(df, team_ids):
	qual = [id for id in team_ids if id in select_qualifiers(df).teamNumber.values]
	scrim = [id for id in team_ids if id in select_scrimmages(df).teamNumber.values and not id in qual]
	none = [id for id in team_ids if id not in qual and id not in scrim]
	classification = {id : 'qualifier' for id in qual}
	classification.update({id : 'scrimmage' for id in scrim})
	classification.update({id : 'none' for id in none})
	return classification

class Report:
	"""
	An event specific scouting report for FTC 2024-2025 Into the Deep.

	Pre:
		Match data stored locally in data/2024 should be up-to-date.

	Args:
		event_code: alphanumeric event identifier string e.g. 'USPAPHQ1'
		ftc_api: an ftc_requests api wrapper object
		guest_teams: dict of teamID:name who are not in the event to add to the report, usefull for looking at the waitlist
	"""
	def __init__(self, event_code, ftc_api: FtcRequests, guest_teams={}):
		self.code = event_code
		team_dict = {}
		if ftc_api:
			team_dict = ftc_api.get_event_teams(event_code)
		team_dict.update(guest_teams)
		team_ids = team_dict.keys()

		event_df = pd.read_pickle('data/2024/events.pkl')
		stats = pd.read_pickle('data/2024/stats.pkl')
		agg_stats = pd.read_pickle('data/2024/agg_stats.pkl')

		team_stats = stats[stats.teamNumber.isin(team_ids)]
		experience_map = classify_teams(team_stats, team_ids)

		calc_category_stats(team_stats, agg_stats)
		calc_auto_oprs(team_stats)
		calc_adj_categories(team_stats)


		# All the calculations should be done by now, just gathering and comparing now
		report = team_stats[['teamNumber','Bucket+','Specimen+','stdDev','AutoBucket','AutoSpecimen','End of Round','Foul','Bucket','Specimen','eventCode']]
		report['Last Data'] = report.eventCode.map(dict(zip(event_df['code'], event_df['date'])))

		agg_report = { x: 'mean' if x == 'stdDev' else 'max' for x in report.columns }
		self.report = report.groupby('teamNumber').agg(agg_report)
		for team in [x[0] for x in experience_map.items() if x[1] == 'none']:
			self.report.loc[team] = [team] + [np.NaN] * 11
		self.report['Experience'] = self.report.teamNumber.map(experience_map)
		self.report.insert(4,'teamName',self.report.teamNumber.map(team_dict))
		self.report.drop(labels=['eventCode','teamNumber'],axis=1,inplace=True)

	def display(self):
		return self.report

	def to_csv(self):
		self.report.to_csv("reports/{}.csv".format(self.code))