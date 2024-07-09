#!/bin/bash
python dbase/init_db.py init
python dbase/loaddata.py
python crmbot/crmbot.py