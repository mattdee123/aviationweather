#!/bin/bash
set -e

DATETIME="$(date +'%Y_%m_%d_%H%M')"
DATE="$(date +'%Y_%m_%d')"

LOG_DIR="/home/mattdee/aviationweather/log/$DATE"
mkdir -p $LOGDIR
LOG_FILE="$LOGDIR/$DATETIME.log"
ERR_FILE="$LOGDIR/$DATETIME.err"
TMP_FILE="/home/mattdee/aviationweather/log/files/$DATETIME.csv"

/home/mattdee/aviationweather/dist/metar_scraper --dburl 'host=/run/postgresql dbname=mattdee sslmode=disable' --filename /home/mattdee/fname --download > $LOG_FILE 2>> $ERR_FILE
