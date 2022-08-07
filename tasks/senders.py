import os
import time

from google.cloud import bigquery as bq
import pandas as pd

from loguru import logger

def leads_sender():

    leads = pd.read_csv("/home/analytics/OddJob/dags/temp_data/leads.csv")

    client = bq.Client()

    start = time.time()

    leads["created_at"] = pd.to_datetime(leads["created_at"])
    leads["updated_at"] = pd.to_datetime(leads["updated_at"])
    leads["closed_at"]  = pd.to_datetime(leads["closed_at"])

    leads.to_gbq(
        "franchise_oddjob.dw_amocrm_fr_leads", if_exists="append"
    )

    end = time.time()

    logger.info(f"Sent {len(leads)} in {end-start} seconds.")


def status_changes_sender():

    events = pd.read_csv("/home/analytics/OddJob/dags/temp_data/status_changes.csv")

    events["created_at"] = pd.to_datetime(events["created_at"])
    events["updated_at"] = pd.to_datetime(events["updated_at"])
    events["closed_at"]  = pd.to_datetime(events["closed_at"])

    client = bq.Client()

    start = time.time()

    events.to_gbq(
    "franchise_oddjob.dw_amocrm_fr_events", if_exists="append"
    )

    end = time.time()

    logger.info(f"Sent {len(events)} in {end-start} seconds.")
