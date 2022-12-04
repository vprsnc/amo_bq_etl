import os
import time

from google.cloud import bigquery as bq
import pandas as pd

from loguru import logger

home = os.getenv("HOME")


def leads_sender(amo):

    logger.add(f"{home}/logs/leads_{amo}.log", backtrace=True, rotation="500 kb")

    # schema = [
    #     {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    #     {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'price', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'responsible_user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'group_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'status_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'pipeline_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    #     {'name': 'loss_reason_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'created_by', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'updated_by', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    #     {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    #     {'name': 'closet_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    #     {'name': 'closest_task_at', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #     {'name': 'is_deleted', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
    #     {'name': 'score', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'account_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'labor_cost', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'traffic_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'resource', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'package', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'utm_source', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'utm_medium', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'utm_campaign', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'utm_term', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'utm_content', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'utm_hueisment', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'utm_campaign_2', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'utm_term_2', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'data_obrasheniya', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    # ]

    leads = pd.read_csv(f"../temp_data/{amo}_leads.csv", low_memory=False)

    client = bq.Client()

    start = time.time()

    # leads["created_at"] = pd.to_datetime(leads["created_at"])
    # leads["updated_at"] = pd.to_datetime(leads["updated_at"])
    # leads["closed_at"]  = pd.to_datetime(leads["closed_at"])

    # leads = leads.astype("str")

    if amo == "franchize":
        dw0 = "franchise"
        dw1 = "fr"
    elif amo == "partner":
        dw0 = "partner"
        dw1 = "ptr"
    elif amo == "accelerator":
        dw0 = "accelerator"
        dw1 = "acl"

    leads.to_gbq(
        f"{dw0}_oddjob.dw_amocrm_{dw1}_leads", if_exists="replace",
        # table_schema=schema
    )

    end = time.time()

    logger.info(f"Sent {len(leads)} in {end-start} seconds.")


def status_changes_sender():

    schema = [
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'type', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'entity_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'entity_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_by', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'account_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'id_status_before', 'type': 'INTEGER', 'mode':'NULLABLE'},
        {'name': 'id_pipeline_before', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'id_status_before', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'id_status_after', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'id_pipeline_after', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ]

    events = pd.read_csv("/home/analytics/OddJob/dags/temp_data/status_changes.csv")

    # events["created_at"] = pd.to_datetime(events["created_at"])
    # events["updated_at"] = pd.to_datetime(events["updated_at"])
    # events["closed_at"]  = pd.to_datetime(events["closed_at"])


    client = bq.Client()

    start = time.time()

    events.to_gbq(
        "franchise_oddjob.dw_amocrm_fr_events", if_exists="replace",
        # table_schema=schema
    )

    end = time.time()

    logger.info(f"Sent {len(events)} in {end-start} seconds.")


def events_sender():
    logger.add(f"{home}/logs/events.log", backtrace=True, rotation="500 kb")
    # schema = [
    #     {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
    #     {'name': 'type', 'type': 'STRING', 'mode': 'REQUIRED'},
    #     {'name': 'entity_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    #     {'name': 'entity_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'created_by', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    #     {'name': 'account_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'id_status_before', 'type': 'INTEGER', 'mode':'NULLABLE'},
    #     {'name': 'id_pipeline_before', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'id_status_before', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'id_status_after', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'id_pipeline_after', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    # ]

    events = pd.read_csv("../temp_data/events.csv")

    client = bq.Client()

    start = time.time()

    events.to_gbq(
        "franchise_oddjob.dw_amocrm_fr_events", if_exists="replace",
        # table_schema=schema
    )

    end = time.time()

    logger.info(f"Sent {len(events)} in {end-start} seconds.")
