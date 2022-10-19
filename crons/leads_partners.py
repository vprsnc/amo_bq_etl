import sys
import os
sys.path.append(os.path.abspath("../tasks"))
sys.path.append(os.path.abspath(".."))

import datetime

from loguru import logger

from leads import store_leads
from senders import leads_sender


home = os.getenv("HOME")
logger.add(f"{home}/logs/leads.log", backtrace=True, rotation="500 kb")
def main():
    store_leads(amo="partner")
    now = datetime.datetime.now()
    logger.info(f"Leads downloaded successfully at {now}")

    leads_sender(amo="partner")
    now = datetime.datetime.now()
    logger.info(f"Leads sent successfully at {now}")


if __name__ == "__main__":
    main()
