import datetime

from events import get_status_changes
from parsers import parse_status_changes
from senders import status_changes_sender

def run_status_changes_etl():
    
    now = datetime.datetime.now()

    events = get_status_changes(now)

    events_df = parse_status_changes(events)

    status_changes_sender(leads_df)
