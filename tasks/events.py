import os
from datetime import timedelta, datetime
from requests.adapters import HTTPAdapter

from loguru import logger
import pandas as pd

from amocrm.v2 import tokens, filters
from amocrm.v2 import Event
from amocrm.v2.interaction import _session


class StatusChanges:


     def get_events(now):
     
         client_id = os.getenv("AMO_CLIENT_ID_2")
         client_secret = os.getenv("AMO_CLIENT_SECRET_2")
         subdomain = os.getenv("AMO_SUBDOMAIN_2")
         redirect_url = os.getenv("AMO_REDIRECT_URI_2")
     
         tokens.default_token_manager(
             client_id=client_id,
             client_secret=client_secret,
             subdomain=subdomain,
             redirect_url=redirect_url,
             storage=tokens.FileTokensStorage(directory_path="/home/analytics/OddJob/tokens/")
             )

         code = os.getenv("CODE_2")

         if code:
              tokens.default_token_manager.init(code)
     
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


     def parse_events(events):

         df = pd.DataFrame(events)
     
         df.drop([
             "value_after", "value_before", "_links", "_embedded"
             ], axis=1, inplace=True)
         df["created_at"] = pd.to_datetime(df["created_at"], unit="s")
     
         return df


     def store_events():

         now = datetime.now()

         events = get_events(now)

         df = parse_events(events)

         df.to_csv("./temp_data/status_changes.csv", index=False)


     def cleanup_events():

         os.remove("./temp_data_events.csv")
