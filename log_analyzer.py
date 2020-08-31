#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

from collections import namedtuple
import datetime
import gzip
import os
from string import Template

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log"
}


def get_date_from_logname(logname):
    """Парсит имя лога и вытаскивает дату"""
    name = logname.rstrip(".gz")
    date_str = name.split("-")[-1]
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:]
    return datetime.date(year=int(year), month=int(month), day=int(day))


def get_latest_log_file(log_dir):
    """Достает последний файл лога"""
    Result = namedtuple("Result", ["name", "path", "date"])
    result = Result(name=None, path=None, date=None)
    empty_result = result
    if not os.path.exists(log_dir):
        return empty_result

    for name in os.listdir(log_dir):
        path = os.path.join(log_dir, name)
        if not os.path.isfile(path):
            continue

        if result.name is None:
            result = Result(name=name, path=path, date=get_date_from_logname(name))
            continue

        current_date = get_date_from_logname(name)
        if current_date > result.date:
            result = Result(name=name, path=path, date=current_date)

    if result.name is None:
        return empty_result

    return result


def get_params(logfile):
    with gzip.open(logfile.path, 'r') as file:
        for row in file:
            yield row


def render_template(logfile):
    # row = [{"count": 2767, "time_avg": 62.994999999999997, "time_max": 9843.5689999999995, "time_sum": 174306.35200000001, "url": "/api/v2/internal/html5/phantomjs/queue/?wait=1m", "time_med": 60.073, "time_perc": 9.0429999999999993, "count_perc": 0.106}]
    f = open("report.html", "r")
    tmp = f.read()
    t = Template(tmp)
    report = t.safe_substitute(table_json=[row for row in get_params(logfile)])
    f.close()
    f = open("test.html", "w")
    f.write(report)
    f.close()


def main():
    logfile = get_latest_log_file(config["LOG_DIR"])
    # get_params(logfile)
    render_template(logfile)


if __name__ == "__main__":
    main()
