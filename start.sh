#!/bin/bash
cd /home/pashbylogan/projects/cypris/cypris-bot/
/home/pashbylogan/apps/miniconda3/envs/cypris-bot/bin/gunicorn wsgi:app
