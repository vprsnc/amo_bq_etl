import datetime

from loguru import logger

from ..tasks.events import store_events
from ..tasks.senders import events_sender


def main():
    logger.add("$HOME/events.log", backtrace=True, rotation="500 MB")
    store_events()
    now = datetime.datetime.now()
    logger.info("Events downloaded successfully at {now}", feature="f-strings")

    events_sender()
    now = datetime.datetime.now()
    logger.info("events sent successfully at {now}", feature="f-strings")


if __name__ == "__main__":
    main()
