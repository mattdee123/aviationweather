#!/bin/bash
set -e

DATETIME="$(date +'%Y_%m_%d_%H%M')"
DATE="$(date +'%Y_%m_%d')"

LOG_DIR="/home/mattdee/aviationweather/log/$DATE"
mkdir -p $LOG_DIR
LOG_FILE="$LOG_DIR/$DATETIME.log"
ERR_FILE="$LOG_DIR/$DATETIME.err"
TMP_FILE="/home/mattdee/aviationweather/log/files/$DATETIME.csv"

/home/mattdee/aviationweather/dist/metar_scraper --dburl 'host=/run/postgresql dbname=mattdee sslmode=disable' --filename $TMP_FILE --download > $LOG_FILE 2>> $ERR_FILE
