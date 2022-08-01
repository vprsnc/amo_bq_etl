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
        storage=tokens.FileTokensStorage(directory_path="/home/analytics/OddJob/tokens/")
    )

#    tokens.default_token_manager.init("def502002127bf3ac43749989230e1ea6a4edab250c8e85d1b198b04010b7c8c934b21dc733cd7d9dc15cdc9a1b43e782a3fc53d977cf153a1355e208128f2a652d518336e241d42571fad091a89dc88b7811b1c0bb6ad211ad897a0e9ffb92de0465f3324e2d3e52031952c358a8109de0843b1aabadd55ba022f8eaf5e55741b8dca64160b50b7d1832e3af81abd1b1e117dd434cdb41481f91bb6f716d477966cea6ebf02348d5ab2c54cd26d8cfbbb0c0984795715b07c2d901a88f5eeb183c586b7afdea8c6e3201c35f696f679342c157e370d0cc9a998588a4de2acff8c8c46f42ebbb9dfa37f1340b10de5b8e3c39b77594871c9f9de1108f62ec4501b14026346dc1427653cbc6e4a6696a1d05b47cc56f0eaa1820fadccd110454c455905f0bcf30e97d0da3d42619872a9c6110526fd60eabfdbe6e988b7f655896b3cdd5b72a4dc3a77ea0f6106e3f802bc94b10d2f73233d3408b3641f93d7a07a5ef918e74c5cb3340e3ce8725eb7837e56c4483ed670e61c5edc999612591f92c8a10dd6c33bbc0a774f1c3456853b17ac4c558940afb4f379d53869c3add2b6dd2a0714c561e335dbfd2547fdb245a4d2f5a19be52f99a65c222e5c1882f9343fe7")

    _session.mount("https://", HTTPAdapter(max_retries=5))

    start = time.time()

    leads = list(Lead.objects.filter(
        
        filters=[filters.DateRangeFilter("updated_at")(now-timedelta(days=1), now)]
    ))

    end = time.time()

    logger.info(f"Downloaded {len(leads)} in {end-start} seconds")

    leads_dicts = []
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

def parse_leads(now):

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

    df.to_csv("./temp_data/leads.csv", index=False)

def cleanup_leads():

    os.remove("./temp_data_leads.csv")
