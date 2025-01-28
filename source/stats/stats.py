import pandas as pd
import numpy as np

# mf: Match Frame: overview of schedule, alliances and topline scores
# qf: Qualifiers Frame: detailed alliance scoring for qualifier matches
# pf: Playoffs Frame: detailed alliance scoring for playoff matches
# returns a Dataframe with detailed match by match breakdown

location_map = { 'NONE' : 0, 'OBSERVATION_ZONE' : 3, 'ASCENT' : 3 }
ascent_map = { 'NONE' : 0, 'OBSERVATION_ZONE' : 3, 'ASCENT_1' : 5, 'ASCENT_2' : 15, 'ASCENT_3' : 30 }
alliance_stats = ['aNet_A', 'aSMPL_A', 'aSMPH_A', 'aSPCL_A', 'aSPCH_A', 'tNet_A', 'tSMPL_A', 'tSMPH_A', 'tSPCL_A', 'tSPCH_A', 'miFoul_A', 'maFoul_A']
non_alliance_labels = ['teamNumber', 'station', 'partnerNumber', 'eventCode', 'playoff', 'win', 'location', 'ascent']

# Statistic helpers
def initialize_team_matrix(team_ids):
	df = pd.DataFrame(columns=team_ids)
	return df.reindex(team_ids).fillna(0)

def schedule_matrix(df, matrix):
	for _, r in df.iterrows():
		matrix.loc[r.teamNumber, r.teamNumber] += 1
		matrix.loc[r.teamNumber, r.partnerNumber] += 1

def aggregate_alliance_stats(df):
	agg_d = { c: 'sum' if c in alliance_stats else 'count' for c in df.columns }
	team_totals = df.groupby(df['teamNumber']).agg(agg_d)
	return team_totals.drop(labels=non_alliance_labels,axis=1)

def calculate_event_opr(df):
	team_matrix = initialize_team_matrix(df.teamNumber.unique())
	schedule_matrix(df, team_matrix)
	pinv = np.linalg.pinv(team_matrix)

	opr = pd.DataFrame(columns=['aNet_O', 'aSMPL_O', 'aSMPH_O', 'aSPCL_O', 'aSPCH_O', 'tNet_O', 'tSMPL_O', 'tSMPH_O', 'tSPCL_O', 'tSPCH_O', 'miFoul_O', 'maFoul_O'])
	opr = opr.reindex(df.teamNumber.unique()).fillna(0)

	team_totals = aggregate_alliance_stats(df)

	#x , r0, r1, s = lstsq(team_matrix.values,team_totals.tSPCH_A.astype(int))
	for stat in alliance_stats:
		opr[stat[:-1] + "O"] = np.matmul(pinv, team_totals[stat]) 
	return opr




def calculate_opr(df):
	oprs = {}
	dfs = [g for _, g in df.groupby('eventCode')]
	for f in dfs:
		opr = calculate_event_opr(f)
		oprs[df.eventCode.values[0]] = opr
	


# Scouting report helpers
def classify_teams(df, team_ids):
	veteran = [id for id in team_ids if id in select_qualifiers(df).teamNumber.values]
	trained = [id for id in team_ids if id in select_scrimmages(df).teamNumber.values and not id in veteran]
	green = [id for id in team_ids if id not in veteran and id not in trained]
	return {'veteran' : veteran, 'trained' : trained, 'green': green}

def select_qualifiers(df):
	return df[df.eventCode.str[-1:] != 'S']

def select_scrimmages(df):
	return df[df.eventCode.str[-1:] == 'S']



def process_event(mf, qf, pf, eventCode):
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


# def aggregate_matches(df):
# 	alliance_stats = ['aNet_A', 'aSMPL_A', 'aSMPH_A', 'aSPCL_A', 'aSPCH_A', 'tNet_A', 'tSMPL_A', 'tSMPH_A', 'tSPCL_A', 'tSPCH_A', 'miFoul_A', 'maFoul_A']
# 	agg_d = { c: 'sum' if c in alliance_stats else 'count' for c in df.columns }
# 	t = df.groupby(df['teamNumber']).agg(agg_d)
# 	non_alliance_labels = ['teamNumber', 'station', 'partnerNumber', 'eventCode', 'playoff', 'win', 'location', 'ascent']
# 	t = t.drop(labels = non_alliance_labels,axis=1)
# 	f = t.div(t.matchNumber,axis=0).drop(labels=['matchNumber'],axis=1)
# 	f['Bucket'] = 2 * (f.aNet_A + f.tNet_A) + 4 * (f.aSMPL_A + f.tSMPL_A) + 8 * (f.aSMPH_A + f.tSMPH_A)
# 	f['Specimen'] = 5 * (f.aSPCL_A + f.tSPCL_A) + 10 * (f.aSPCH_A + f.tSPCH_A)
# 	f['Ascent'] = location_map[f.location] + ascent_map[f.ascent]
# 	f['Foul'] = -5 * f.miFoul_A + -15 * f.maFoul_A
# 	h = f.drop(labels=alliance_stats,axis=1)
# 	return None

def generate_event_scouting_report(df, team_ids):
	return None