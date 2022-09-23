import datetime

from loguru import logger

from tasks.leads import store_leads
from tasks.senders import leads_sender


def main():
    logger.add("$HOME/leads.log", backtrace=True, rotation="500 MB")
    store_leads()
    now = datetime.datetime.now()
    logger.info("Leads downloaded successfully at {now}", feature="f-strings")

    leads_sender()
    now = datetime.datetime.now()
    logger.info("Leads sent successfully at {now}", feature="f-strings")


if __name__ == "__main__":
    main()
