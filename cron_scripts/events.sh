#!/bin/sh

mydate=`date`
cd $HOME/OddJob/crons
$HOME/OddJob/venv/bin/python $HOME/OddJob/crons/events_cron.py
echo "events cron successful on $mydate!" >> $HOME/test.cron
