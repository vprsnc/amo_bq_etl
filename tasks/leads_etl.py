import datetime

from amocrm.v2.exceptions import NoToken
from amocrm.v2 import tokens

from leads import get_leads
from parsers import parse_leads
from senders import leads_sender


def run_leads_etl():
    
    now = datetime.datetime.now()

    leads = get_leads(now)
    
    leads_df = parse_leads(leads)

    leads_sender(leads_df)
