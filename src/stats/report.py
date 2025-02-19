import pandas as pd
import numpy as np
import os
from src.ftc_api.ftc_requests import FtcRequests
from src.stats.stats import update_statistics


# def calc_auto_oprs(stats):
# 	stats['AutoBucket'] = 2 * stats.aNet_O + 4 * stats.aSMPL_O+ 8 * stats.aSMPH_O
# 	stats['AutoSpecimen'] = 5 * stats.aSPCL_O + 10 * stats.aSPCH_O

# def calc_adj_categories(stats):
# 	stats['Bucket+'] = stats.Bucket + stats.Foul + stats['End of Round']
# 	stats['Specimen+'] = stats.Specimen + stats.Foul + stats['End of Round']

# Classify teams
def select_qualifiers(df):
	return df[df.eventCode.str[-1:] != 'S']

def select_scrimmages(df):
	return df[df.eventCode.str[-1:] == 'S']

def classify_teams(df: pd.DataFrame, team_ids: list):
	"""
	Classifys teams by the most advanced type of event they've atteneded this year.

	Args:
		df: a DataFrame with 'teamNumber' and 'eventCode' columns
		team_ids: list of team_ids to compare
	"""

	mf = df.reset_index()
	qual = [id for id in team_ids if id in select_qualifiers(mf).teamNumber.values]
	scrim = [id for id in team_ids if id in select_scrimmages(mf).teamNumber.values and not id in qual]
	none = [id for id in team_ids if id not in qual and id not in scrim]
	classification = {id : 'qualifier' for id in qual}
	classification.update({id : 'scrimmage' for id in scrim})
	classification.update({id : 'none' for id in none})
	return classification

# Stat fxns
def pool_pts_variance(df, teams):
	df['un_bessel'] = (df['matchNumber'] - 1) * (df['σ Pts'])**2
	n = teams['matchNumber'].sum()
	pooled_variance_numerator = teams['un_bessel'].sum()
	pooled_variance = pooled_variance_numerator / (n - 1)
	std_dev = np.sqrt(pooled_variance)
	return std_dev

def recombine_variance(df, std_dev):
	df['σ Pts'] = std_dev
	df[['Auto', 'EndGame', 'Fouls', 'x̄ Pts']] = df[['Auto', 'EndGame', 'Fouls', 'x̄ Pts']].divide(df.matchNumber,axis='index')

# Private fxns
def beautify_matches(df: pd.DataFrame, team_dict):
	df.insert(loc=1,column='Team Name',value=df.teamNumber.map(team_dict))
	df.insert(loc=4,column='Partner Name',value=df.partnerNumber.map(team_dict))
	df.rename(columns={'teamNumber': 'Team ID', 'partnerNumber': 'Partner ID', 'matchNumber': 'Match #'},inplace=True)
	df.drop(columns=['eventCode'],inplace=True)
	return df

def limit_noise_variance(stats: pd.DataFrame):
		stats.loc[pd.IndexSlice[:,:,0], 'σ Bucket'] = 0
		stats.loc[pd.IndexSlice[:,:,1], 'σ Specimen'] = 0



class Report:
	"""
	An event specific scouting report for FTC 2024-2025 Into the Deep.

	Pre:
		Match data stored locally in data/2024 should be up-to-date.

	Args:
		event_code: alphanumeric event identifier string e.g. 'USPAPHQ1'
	"""
	def __init__(self, event_code):
		self.code = event_code
		self.report = pd.DataFrame()
		self.stat_filter = ['matchNumber', 'Auto', 'EndGame', 'Fouls', 'x̄ Bucket', 'x̄ Specimen', 'x̄ Pts', 'σ Bucket',  'σ Specimen', 'σ Pts', 'Max Bucket', 'Max Specimen', 'Max Pts']
		self.report_filter = ['σ Bucket', 'x̄ Bucket', 'teamNumber', 'Team Name', 'σ Specimen', 'x̄ Specimen', 'x̄ Pts', 'σ Pts', 'Max Pts', 'Auto', 'EndGame', 'Fouls']

	# Helpers
	def prep_for_aggregation(self, stats: pd.DataFrame):
		stats = stats[self.stat_filter]
		stats[['Auto', 'EndGame', 'Fouls', 'x̄ Pts']] = stats[['Auto', 'EndGame', 'Fouls', 'x̄ Pts']].multiply(stats.matchNumber,axis='index')
		return stats
	
	# Output
	def display(self):
		return self.report

	def to_csv(self):
		directory = os.path.dirname(f"reports/{self.code}.csv")
		os.makedirs(directory, exist_ok=True)
		self.report.to_csv(f"reports/{self.code}.csv")

class Prelook(Report):
	"""
	An event specific scouting report that precedes the event
	"""
	def __init__(self, event_code, team_dict):
		super.__init__(event_code)
		team_ids = team_dict.keys()

		event_df = pd.read_pickle('data/2024/events.pkl')
		stats = pd.read_pickle('data/2024/disagg_stats.pkl')
		exp_map = classify_teams(stats, team_ids)

		team_stats = stats.loc[team_ids]
		limit_noise_variance(team_stats)
		std_dev = pool_pts_variance(team_stats, team_stats.groupby(['teamNumber', 'eventCode']))
		team_stats = self.prep_for_aggregation(team_stats)
		agg_team_data = { x: 'sum' if x in ['matchNumber','Auto', 'EndGame', 'Fouls', 'x̄ Pts'] else 'max' for x in team_stats.columns }
		team_stats = team_stats.groupby(['teamNumber', 'eventCode']).agg(agg_team_data)
		recombine_variance(team_stats, std_dev)

		agg_potential = { x: 'mean' if x in ['Fouls'] else 'max' for x in team_stats.columns } # TODO get variance on Buckets and Specimens where the max happens
		team_stats = team_stats.groupby('teamNumber').agg(agg_potential)

		#TODO make report stuff pretty
		# report['Last Data'] = report.eventCode.map(dict(zip(event_df['code'], event_df['date'])))
		
		# for team in [x[0] for x in experience_map.items() if x[1] == 'none']:
		# 	self.report.loc[team] = [team] + [np.NaN] * 11

		# self.report['Experience'] = self.report.teamNumber.map(experience_map)
		# self.report.insert(4,'Team Name',self.report.teamNumber.map(team_dict))
		# self.report.drop(labels=['eventCode','teamNumber'],axis=1,inplace=True)
		

class Live_Report(Report):
	"""
	An event specific scouting report that uses live data during the event
	"""
	def __init__(self, event_code, ftc_api: FtcRequests):
		super.__init__(event_code)
		team_dict = ftc_api.get_event_teams(event_code)
		update_statistics(event_code, ftc_api)

		stats = pd.read_pickle('data/2024/disagg_stats.pkl')
		stats = stats.xs(self.eventCode,level='eventCode')

		limit_noise_variance(stats)
		std_dev = pool_pts_variance(stats, stats.groupby('teamNumber'))
		stats = self.prep_for_aggregation(stats)
		agg_team_data = { x: 'sum' if x in ['matchNumber','Auto', 'EndGame', 'Fouls', 'x̄ Pts'] else 'max' for x in stats.columns }
		stats = stats.groupby('teamNumber').agg(agg_team_data)
		recombine_variance(stats, std_dev)
		stats['Team Name'] = stats.index.map(team_dict)
		stats.reset_index(inplace=True)
		self.report = stats[self.report_filter].copy()

		# Get match regression data
		disagg_matches = pd.read_pickle('data/2024/disagg_matches.pkl')
		disagg_matches = disagg_matches[disagg_matches.eventCode == self.eventCode]
		max_stats = disagg_matches.groupby('teamNumber').agg('max')
		self.report.insert(0, 'Max High Samples', max_stats.reset_index().tSMPH)
		self.report.insert(7, 'Max High Specimens', max_stats.reset_index().tSPCH)

		# Get match data
		matches = pd.read_pickle('data/2024/matches.pkl')
		matches = matches[matches.eventCode == self.eventCode]
		self.matches = beautify_matches(matches, team_dict)

		# Get pretty regression data
		disagg_matches = beautify_matches(disagg_matches, team_dict)
		disagg_matches.insert(7,'Points',disagg_matches.pop('Pts'))
		disagg_matches.drop(columns=disagg_matches.columns[-7:],inplace=True)
		self.disagg_matches = disagg_matches