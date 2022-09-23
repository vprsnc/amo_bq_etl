import sys
import os
sys.path.append(os.path.abspath("../tasks"))
sys.path.append(os.path.abspath(".."))

import datetime

from loguru import logger

from events import store_events
from senders import events_sender


def main():
    logger.add("~/logs/events.log", backtrace=True, rotation="500 MB")
    store_events()
    now = datetime.datetime.now()
    logger.info(f"Events downloaded successfully at {now}")

    events_sender()
    now = datetime.datetime.now()
    logger.info(f"Events sent successfully at {now}")


if __name__ == "__main__":
    main()
