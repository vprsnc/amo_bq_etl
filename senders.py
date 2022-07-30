import os
import time

from google.cloud import bigquery as bq
import pandas as pd

from loguru import logger

def leads_sender(leads):

    client = bq.Client()

    start = time.time()

    leads.to_gbq(
        "franchise_oddjob.dw_amocrm_fr_leads", if_exists="append"
    )

    end = time.time()

    logger.info(f"Sent {len(leads)} in {end-start} seconds.")


def status_changes_sender(events):

    client = bq.Client()

    start = time.time()

    events.to_gbq(
    "franchise_oddjob.dw_amocrm_fr_events", if_exists="append"
    )

    end = time.time()

    logger.info(f"Sent {len(events)} in {end-start} seconds.")
