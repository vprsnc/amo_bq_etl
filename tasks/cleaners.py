import datetime
import os

from loguru import logger


def cleanup_events():
    logger.add("~/logs/events.log", backtrace=True, rotation="500 MB")
    size = os.path.getsize("../temp_data/events.csv")
    logger.info(f"{size} were removed from disk")
    os.remove("../temp_data/events.csv")

    
def cleanup_leads():
    logger.add("~/logs/leads.log", backtrace=True, rotation="500 MB")
    size = os.path.getsize("../temp_data/leads.csv")
    logger.info(f"{size} were removed from disk")
    os.remove("../temp_data/leads.csv")
