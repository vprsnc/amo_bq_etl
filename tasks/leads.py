import os
from requests.adapters import HTTPAdapter
from datetime import timedelta, datetime
import time

import pandas as pd
from loguru import logger
from amocrm.v2 import tokens, filters
from amocrm.v2.interaction import _session

from models import Lead

def get_leads(now):

    client_id = os.getenv("AMO_CLIENT_ID")
    client_secret = os.getenv("AMO_CLIENT_SECRET")
    subdomain = os.getenv("AMO_SUBDOMAIN")
    redirect_url = os.getenv("AMO_REDIRECT_URI")

    tokens.default_token_manager(
        client_id=client_id,
        client_secret=client_secret,
        subdomain=subdomain,
        redirect_url=redirect_url,
        storage=tokens.FileTokensStorage(directory_path="/home/analytics/OddJob/tokens/leads/")
    )

    # code should be injected in the env if there's no refresh code in tokens folder,
    # e.g. when utilising the new env.
    code = os.getenv("CODE")

    if code:
        tokens.default_token_manager.init(code)
        logger.debug("Access token has been used!")

    # Incresing the amount of tries in the requests library
    _session.mount("https://", HTTPAdapter(max_retries=5))

    start = time.time()

    # leads = list(Lead.objects.filter(
        
    #     filters=[filters.DateRangeFilter("updated_at")(now-timedelta(days=1), now)]
    # ))

    # TODO as soon as DB grows, we need to return back to the old way of downloading leads
    # per 24 hr (ABOVE)
    leads = list(Lead.objects.all())

    end = time.time()

    logger.info(f"Downloaded {len(leads)} in {end-start} seconds")

    leads_dicts = []
    counter = 0
    for lead in leads:
        leads_dicts.append(lead.__dict__['_data'])
        try:
            leads_dicts[counter]["traffic_type"] = lead.tip_traffika.__dict__["value"]
        except AttributeError:
            leads_dicts[counter]["traffic_type"] = ""
        leads_dicts[counter]["city"] = lead.gorod
        try:
            leads_dicts[counter]["resource"] = lead.resurs.__dict__["value"]
        except AttributeError:
            leads_dicts[counter]["resource"] = ""
        try:
            leads_dicts[counter]["package"] = lead.paket.__dict__["value"]
        except AttributeError:
            leads_dicts[counter]["package"] = ""
        counter += 1

    custom_fields_needed = [
        "Дата прихода обращения", "utm_source", "utm_medium", "utm_campaign", 
        "utm_term", "utm_content", "utm_hueisment", "utm_campaign (2)", 
        "utm_term (2)"
       ]

    for lead in leads_dicts:
        if lead["custom_fields_values"] != None:
            for field in lead["custom_fields_values"]:
                for field_name in custom_fields_needed:
                    if field["field_name"] == field_name:
                         lead[field_name] = field["values"][0]["value"]

    return leads_dicts

def parse_leads(leads):

    df = pd.DataFrame(leads)

    df ["data_obrasheniya"] = df["Дата прихода обращения"].fillna(0).astype("int")

    df.drop([
        "_links", "_embedded", "custom_fields_values", "Дата прихода обращения"
       ], axis=1, inplace=True)


    df["created_at"] = pd.to_datetime(df["created_at"], unit="s")
    df["updated_at"] = pd.to_datetime(df["updated_at"], unit="s")
    df["closed_at"]  = pd.to_datetime(df["closed_at"], unit="s")

    df.rename({
        "utm_campaign (2)": "utm_campaign_2",
        "utm_term (2)": "utm_term_2",
        "price": "budget"
       }, axis=1, inplace=True)

    return df


def store_leads():

    now = datetime.now()

    leads = get_leads(now)

    df = parse_leads(leads)

    df.to_csv("../temp_data/leads.csv", index=False)

# def cleanup_leads():

#     os.remove("/home/analytics/OddJob/dags/temp_data/leads.csv")
