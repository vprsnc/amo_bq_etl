#!/bin/sh

mydate=`date`
cd $HOME/OddJob/crons
$HOME/OddJob/venv/bin/python $HOME/OddJob/crons/leads_cron.py
echo "leads cron successful on $mydate!" >> $HOME/test.cron
