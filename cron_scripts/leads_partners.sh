#!/bin/sh

mydate=`date`
cd $HOME/OddJob/crons
$HOME/OddJob/venv/bin/python $HOME/OddJob/crons/leads_partners.py
if [ $? -eq 0 ]; then
    echo "leads cron successful on $mydate" >> $HOME/test.cron
else
    echo "leads cron faield on $mydate" >> $HOME/test.cron
