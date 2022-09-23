import os
from datetime import datetime, time

from google.oauth2 import service_account

import gspread
import gspread_dataframe as gd
import pandas as pd


def get_events(now):
    gc = gspread.service_account(filename="../tokens/yet-another-python-c9430ad455a2.json")

    gsheet = gc.open_by_url(
         "https://docs.google.com/spreadsheets/d/1sv5NZ35p4p4dPJrqczZwjt-a1YvaO9D18yALfCy3fus/edit#gid=533816979"
   )

    
    events= gsheet.worksheet("events").get_all_records()
    return events


def parse_events(events):
    events_df = pd.DataFrame(events)
    events_df.rename(
        {
            "ID сущности": "lead_id",
            "Имя сущности": "event_type",
            "Имя предедущей воронки": "funnel_name_prev",
            "Имя предедущего статуса": "status_name_prev",
            "Имя новой воронки": "funnel_name",
            "Имя нового статуса": "status_name",
            "Имя обновителя": "modified_by",
            "Дата создания": "date_created"
           }, axis=1, inplace=True
       )


def store_events():
    now = datetime.now
    events = get_events(now)
    df = parse_events(events)
    df.to_csv(
        "home/analytics/OddJob/dags/temp_data/events.csv",
        index=False
    )

def cleanup_events():
    os.remove("home/analytics/OddJob/dags/temp_data/events.csv")
