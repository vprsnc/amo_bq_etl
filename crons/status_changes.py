import sys
import os
sys.path.append(os.path.abspath("../tasks"))
# sys.path.append(os.path.abspath(".."))

import datetime

from loguru import logger

# from status_changes import store_events
import status_changes
from senders import events_sender

home = os.getenv("HOME")
logger.add(f"{home}/logs/events.log", backtrace=True, rotation="500 kb")

def main():
    status_changes.store_events()
    now = datetime.datetime.now()
    logger.info(f"Events downloaded successfully at {now}")

    events_sender()
    now = datetime.datetime.now()
    logger.info(f"Events sent successfully at {now}")


if __name__ == "__main__":
    main()
