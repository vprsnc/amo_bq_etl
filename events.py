import os
from datetime import timedelta
from requests.adapters import HTTPAdapter

import loguru

from amocrm.v2 import tokens, filters
from amocrm.v2 import Event
from amocrm.v2.interaction import _session

from auth import amo_auth

def get_status_changes(now):


    client_id = os.getenv("AMO_CLIENT_ID")
    client_secret = os.getenv("AMO_CLIENT_SECRET")
    subdomain = os.getenv("AMO_SUBDOMAIN")
    redirect_url = os.getenv("AMO_REDIRECT_URI")

    tokens.default_token_manager(
        client_id=client_id,
        client_secret=client_secret,
        subdomain=subdomain,
        redirect_url=redirect_url,
        storage=tokens.FileTokensStorage(directory_path="/home/analytics/OddJob/tokens/")
    )

    _session.mount("https://", HTTPAdapter(max_retries=5))

    start = time.time()

    events = list(Event.objects.filter(
        filters=[
            filters.DateRangeFilter("created_at")(now-timedelta(days=1), now),
            filters.SingleFilter("type")("lead_status_changed")
           ]
       ))

    end = time.time()

    logger.info(f"Downloaded {len(events)} in {end-start} seconds")

    events_dicts = []


    for event in events:

        events_dicts.append(event.__dict__['_data'])


    for event in events_dicts:

        event["id_status_before"] = event["value_before"][0]["lead_status"]["id"]
        event["id_pipeline_before"] = event["value_before"][0]["lead_status"]["pipeline_id"]
        event["id_status_after"] = event["value_after"][0]["lead_status"]["id"]
        event["id_pipeline_after"] = event["value_after"][0]["lead_status"]["pipeline_id"]

    return events_dicts
