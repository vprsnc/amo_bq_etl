#!/bin/sh

mydate=`date`
cd $HOME/OddJob/crons
$HOME/OddJob/venv/bin/python $HOME/OddJob/crons/events_cron.py
if [ $? -eq 0 ]; then
    echo "events cron successful on $mydate" >> $HOME/test.cron
else
    echo "events cron faield on $mydate" >> $HOME/test.cron
