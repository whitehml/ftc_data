import pandas as pd

from src.ftc_api.ftc_requests import FtcRequests
from src.stats import stats
from src.stats.report import Report

team_filter = ['matchNumber', 'win', 'location', 'ascent', 'Auto', 'EndGame', 'Fouls', 'x̄ Bucket', 'x̄ Specimen', 'x̄ Pts', 'σ Bucket', 'σ Specimen', 'σ Pts', 'Max Bucket', 'Max Specimen', 'Max Pts']

# User terminal function to access code

### Access Database
def view_events():
	return pd.read_pickle('data/2024/events.pkl')

def view_matches():
	return pd.read_pickle('data/2024/matches.pkl')

def view_stats():
	return pd.read_pickle('data/2024/stats.pkl')

def view_aggregated_stats():
	return pd.read_pickle('data/2024/agg_stats.pkl')

## Update Database
def update_database(event_codes=None):
	if not event_codes:
		event_codes = input("Please enter each event code separated by a space: ")
	stats.update_statistics(event_codes, FtcRequests())

def scout_event(event_code, team_dict):
	report = Report(event_code, None, team_dict)
	report.to_csv()
	return report.display()

def scout_event_live(event_code, waitlist_dict={}):
	report = Report(event_code, FtcRequests(), waitlist_dict)
	report.to_csv()
	return report.display()
