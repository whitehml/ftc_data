﻿import pandas as pd

# mf: Match Frame: overview of schedule, alliances and topline scores
# qf: Qualifiers Frame: detailed alliance scoring for qualifier matches
# pf: Playoffs Frame: detailed alliance scoring for playoff matches
# returns a Dataframe with detailed match by match breakdown
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

	# Pre-Process playoffs
	pf.matchNumber = pf.matchSeries

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
	process_stage(True, pf)

	return df