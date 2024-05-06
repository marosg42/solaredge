#!/usr/bin/env python3

import logging
from datetime import datetime, timedelta
from dateutil import parser
from influxdb import InfluxDBClient
import sys
import argparse
import requests
import json
import os

tzinfo = {
    "CEST": 2 * 3600,
    "CET": 1 * 3600,
}


def get_data(args):
    HEADERS = {
        "User-Agent": "App",
        "Accept-Encoding": "gzip",
        "Connection": "Keep-Alive",
    }   
    DELTAS = {
        "hour":  timedelta(hours = 1),
        "day": timedelta(days = 1),
        "week": timedelta(weeks = 1),
    }
    now = datetime.now()
    old = now - DELTAS[args.interval] 
    now += timedelta(hours = 2)
    date_str = "startTime=" + old.strftime("%Y-%m-%d%%20%H:%M:00")
    date_str += "&endTime=" + now.strftime("%Y-%m-%d%%20%H:%M:00")
    uri = f"/site/{args.site}/power?{date_str}&api_key={args.api_key}"
    fullurlstring = "https://monitoringapi.solaredge.com" + uri
    r = requests.get(url=fullurlstring, headers=HEADERS)
    logging.info(fullurlstring)
    if r.status_code != 200:
        logging.info(r.status_code)
        logging.info(r.text)
        sys.exit(1)
    return json.loads(r.text)["power"]["values"]


def upload_data(data):
    database="elektrina"
    client = InfluxDBClient(host=os.environ["DOCKER_IP"], port=8086)
    client.create_database(database)
    client.switch_database(database)

    records = []
    for i in data:
        if not i["value"]:
            continue
        timestamp = int(datetime.timestamp(parser.parse(i["date"]+" CEST", tzinfos=tzinfo)))
        records.append(f"kW,type=solar value={i['value']/1000} {timestamp}")
    client.write_points(records, database=database, protocol="line", time_precision="s")


def get_parser(args):
    parser = argparse.ArgumentParser(
        description=""
    )
    parser.add_argument('interval', choices=('hour', 'day', 'week'))
    required = parser.add_argument_group('required arguments')
    required.add_argument(
        "--site",
        help="",
        required=True
    )
    required.add_argument(
        "--api_key",
        help="",
        required=True
    )
    return parser.parse_args(args)


def main(sys_args):
    args = get_parser(sys_args)
    data = get_data(args)
    upload_data(data)


if __name__ == "__main__":
    format = "%(asctime)s %(message)s"
    logging.basicConfig(
        format=format, level=logging.INFO, datefmt="[%Y-%m-%d %H:%M:%S]"
    )
    main(sys.argv[1:])
