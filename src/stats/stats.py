﻿import pandas as pd
import numpy as np
from src.ftc_api.ftc_requests import FtcRequests

location_map = { 'NONE' : 0, 'OBSERVATION_ZONE' : 3, 'ASCENT' : 3 }
ascent_map = { 'NONE' : 0, 'OBSERVATION_ZONE' : 3, 'ASCENT_1' : 5, 'ASCENT_2' : 15, 'ASCENT_3' : 30 }
alliance_stats = ['aNet_A', 'aSMPL_A', 'aSMPH_A', 'aSPCL_A', 'aSPCH_A', 'tNet_A', 'tSMPL_A', 'tSMPH_A', 'tSPCL_A', 'tSPCH_A', 'miFoul_A', 'maFoul_A']
non_alliance_labels = ['teamNumber', 'station', 'partnerNumber', 'eventCode', 'playoff', 'win', 'location', 'ascent']

# Statistic helpers
def initialize_team_matrix(team_ids):
	team_ids.sort()
	df = pd.DataFrame(columns=team_ids)
	return df.reindex(team_ids).fillna(0).sort_index()

def schedule_matrix(df, matrix):
	for _, r in df.iterrows():
		matrix.loc[r.teamNumber, r.teamNumber] += 1
		matrix.loc[r.teamNumber, r.partnerNumber] += 1

def aggregate_alliance_stats(df):
	agg_d = { c: 'sum' if c in alliance_stats else 'count' for c in df.columns }
	team_totals = df.groupby(df['teamNumber']).agg(agg_d)
	return team_totals.drop(labels=non_alliance_labels,axis=1)

def calculate_event_opr(df, eventCode):
	team_matrix = initialize_team_matrix(df.teamNumber.unique())
	schedule_matrix(df, team_matrix)
	pinv = np.linalg.pinv(team_matrix)

	opr = pd.DataFrame(columns=['eventCode', 'aNet_O', 'aSMPL_O', 'aSMPH_O', 'aSPCL_O', 'aSPCH_O', 'tNet_O', 'tSMPL_O', 'tSMPH_O', 'tSPCL_O', 'tSPCH_O', 'miFoul_O', 'maFoul_O'])
	opr = opr.reindex(df.teamNumber.unique()).fillna(0)
	opr.sort_index(inplace=True)

	team_totals = aggregate_alliance_stats(df)

	for stat in alliance_stats:
		opr[stat[:-1] + "O"] = np.matmul(pinv, team_totals[stat])
	opr['eventCode'] = [eventCode] * len(opr)
	opr.reset_index(inplace=True)
	return opr

def aggregate_event_matches(df):
	agg_event = { x: 'sum' if x in ['location' , 'ascent', 'win'] else 'first' if x == 'stdDev' else 'count' for x in df.columns }
	return df.groupby(['eventCode', 'teamNumber','playoff']).agg(agg_event).drop(columns=['teamNumber', 'station', 'partnerNumber', 'eventCode', 'playoff', 'npPts'] + alliance_stats)

# Pre: a match data dataframe, with playoff data removed
def calculate_opr(df):
	oprs = []
	dfs = [g for _, g in df.groupby('eventCode')]
	for f in dfs:
		oprs.append(calculate_event_opr(f, f.eventCode.values[0]))
	return oprs
		
	
# Scouting report helpers
def calc_npPts(df):
	"""# of points scored by your alliance, minus opposing fouls and ally ascent points."""
	df = df.replace({'location':location_map, 'ascent':ascent_map})
	df['npPts'] = 2 * (df.aNet_A + df.tNet_A) + 4 * (df.aSMPL_A + df.tSMPL_A) + 8 * (df.aSMPH_A + df.tSMPH_A) + 5 * (df.aSPCL_A + df.tSPCL_A) + 10 * (df.aSPCH_A + df.tSPCH_A) + df.location + df.ascent
	return df

def std_by_event(df):
	std_values = df.groupby(["eventCode", "teamNumber", "playoff"])["npPts"].std()
	for i, r in df.iterrows():
		df.loc[i, 'stdDev'] = std_values.loc[r.eventCode, r.teamNumber, r.playoff]

def process_event(mf, qf, pf, eventCode):
	"""
	Returns a Dataframe with detailed match by match breakdown.

	mf: Match Frame: overview of schedule, alliances and topline scores
	qf: Qualifiers Frame: detailed alliance scoring for qualifier matches
	pf: Playoffs Frame: detailed alliance scoring for playoff matches
	"""

	# Pre-Process matches
	mf['red1'] = [0] * len(mf)
	mf['red2'] = [0] * len(mf)
	mf['blue1'] = [0] * len(mf)
	mf['blue2'] = [0] * len(mf)
	mf.loc[mf.series > 0, 'matchNumber'] = mf.series

	# Flatten team ID data
	for i, row in mf.iterrows():
		tf = pd.DataFrame(row.teams)
		mf.at[i, 'red1'] = tf[tf.station == 'Red1'].teamNumber
		mf.at[i, 'red2'] = tf[tf.station == 'Red2'].teamNumber
		mf.at[i, 'blue1'] = tf[tf.station == 'Blue1'].teamNumber
		mf.at[i, 'blue2'] = tf[tf.station == 'Blue2'].teamNumber

	df = pd.DataFrame(columns=['teamNumber', 'station', 'partnerNumber', 'eventCode', 'matchNumber', 'playoff', 'win', 'location', 'ascent', 'aNet_A', 'aSMPL_A', 'aSMPH_A', 'aSPCL_A', 'aSPCH_A', 'tNet_A', 'tSMPL_A', 'tSMPH_A', 'tSPCL_A', 'tSPCH_A', 'miFoul_A', 'maFoul_A'])

	# f: a stage frame, either playoffs or qualifiers
	def process_stage(is_playoff, f):
		for i, row in f.iterrows():
			sf = pd.DataFrame(row.alliances)

			offset = i * 4 + len(df)
			level = "PLAYOFF" if is_playoff else "QUALIFICATION"
			alliance_win = sf.loc[sf.alliance == 'Red', 'totalPoints'].values[0] - sf.loc[sf.alliance == 'Blue', 'totalPoints'].values[0]

			for index, station, partner, win in zip([0,1,2,3], ['red1', 'red2', 'blue1', 'blue2'], ['red2', 'red1', 'blue2', 'blue1'], [alliance_win > 0, alliance_win > 0, alliance_win < 0, alliance_win < 0]):
				alliance = station[0].upper() + station[1:-1]
				robot = "robot" + station[-1:]
				df.at[offset + index, 'teamNumber'] = mf.loc[(mf.matchNumber == row.matchNumber) & (mf.tournamentLevel == level), station].values[0]
				df.at[offset + index, 'station'] = station
				df.at[offset + index, 'partnerNumber'] = mf.loc[(mf.matchNumber == row.matchNumber) & (mf.tournamentLevel == level), partner].values[0]
				df.at[offset + index, 'eventCode'] = eventCode
				df.at[offset + index, 'matchNumber'] = row.matchNumber
				df.at[offset + index, 'playoff'] = is_playoff
				df.at[offset + index, 'win'] = win
				df.at[offset + index, 'location'] = sf.loc[sf.alliance == alliance, robot + 'Auto'].values[0]
				df.at[offset + index, 'ascent'] = sf.loc[sf.alliance == alliance, robot + 'Teleop'].values[0]
				df.at[offset + index, 'aNet_A'] = sf.loc[sf.alliance == alliance, 'autoSampleNet'].values[0]
				df.at[offset + index, 'aSMPL_A'] = sf.loc[sf.alliance == alliance, 'autoSampleLow'].values[0]
				df.at[offset + index, 'aSMPH_A'] = sf.loc[sf.alliance == alliance, 'autoSampleHigh'].values[0]
				df.at[offset + index, 'aSPCL_A'] = sf.loc[sf.alliance == alliance, 'autoSpecimenLow'].values[0]
				df.at[offset + index, 'aSPCH_A'] = sf.loc[sf.alliance == alliance, 'autoSpecimenHigh'].values[0]
				df.at[offset + index, 'tNet_A'] = sf.loc[sf.alliance == alliance, 'teleopSampleNet'].values[0]
				df.at[offset + index, 'tSMPL_A'] = sf.loc[sf.alliance == alliance, 'teleopSampleLow'].values[0]
				df.at[offset + index, 'tSMPH_A'] = sf.loc[sf.alliance == alliance, 'teleopSampleHigh'].values[0]
				df.at[offset + index, 'tSPCL_A'] = sf.loc[sf.alliance == alliance, 'teleopSpecimenLow'].values[0]
				df.at[offset + index, 'tSPCH_A'] = sf.loc[sf.alliance == alliance, 'teleopSpecimenHigh'].values[0]
				df.at[offset + index, 'miFoul_A'] = sf.loc[sf.alliance == alliance, 'minorFouls'].values[0]
				df.at[offset + index, 'maFoul_A'] = sf.loc[sf.alliance == alliance, 'majorFouls'].values[0]

	process_stage(False, qf)
	if not pf.empty:
		pf.matchNumber = pf.matchSeries
		process_stage(True, pf)

	return df

def update_matches(event_list, ftc_api: FtcRequests) -> pd.DataFrame:
	event_data = ftc_api.get_events_data(event_list)
	match_data = [pd.read_pickle('data/2024/matches.pkl')]
	for code, event in zip(event_list, event_data):
		match_data.append(process_event(event['mf'], event['qf'], event['pf'], code))
	mf = pd.concat(match_data).reset_index(drop=True).drop_duplicates()
	mf.to_pickle('data/2024/matches.pkl')
	return mf

def update_opr(matches):
	oprs = calculate_opr(matches)
	df = pd.concat(oprs).reset_index(drop=True)
	df = df.rename({'index':'teamNumber'},axis=1)
	df.to_pickle('data/2024/stats.pkl')

def update_aggregations(matches):
	matches = calc_npPts(matches)
	std_by_event(matches)
	matches.win = matches.win.astype(int)
	agg = aggregate_event_matches(matches)
	agg.to_pickle('data/2024/agg_stats.pkl')

def update_statistics(event_list, ftc_api: FtcRequests):
	"""Updates FTC Into the Deep tracked statistics for events listed by ID. 2024 only."""
	matches = update_matches(event_list, ftc_api)
	matches.playoff = matches.playoff.astype('bool')
	matches = matches[~matches.playoff]
	update_opr(matches)
	update_aggregations(matches)