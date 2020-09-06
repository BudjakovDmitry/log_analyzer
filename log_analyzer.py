#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

from collections import namedtuple
import argparse
from datetime import date
import gzip
import logging
import os
import re
from string import Template

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "VALID_LOG_FORMATS": ["gz", "log"],
    "LOR_OUTPUT_DIR": "./output"
}

filename = config.get("LOG_OUTPUT_DIR")
LOG_FORMAT = "[%(asctime)s] %(levelname).1s %(message)s"
logging.basicConfig(format=LOG_FORMAT, filename=filename)


def is_ui_log(file_name):
    """
    Проверяет, что данный лог является логом интерфейса
    """
    name = file_name.split(".")[0]
    type_ = name.split("-")[-1]
    return type_ == "ui"


def is_valid_format(file_name, valid_formats):
    """Проверяет, что лог имеет корректный формат"""
    suf = file_name.split(".")[-1]
    for ext in valid_formats:
        if suf.startswith(ext):
            return True

    return False


def get_date_from_log_name(log_name):
    """Вытаскивает дату создания из имени лога"""
    date_ = re.search(r"\d+", log_name).group()
    year = date_[:4]
    month = date_[4:6]
    day = date_[6:]
    return date(year=int(year), month=int(month), day=int(day))


def get_latest_log_file(log_dir, valid_formats):
    """Просматривает папку с логами и находит самый свежий"""
    Result = namedtuple("Result", ["name", "path", "date"])
    result = Result(name=None, path=None, date=None)
    empty_result = result
    if not os.path.exists(log_dir):
        return empty_result

    for name in os.listdir(log_dir):
        path = os.path.join(log_dir, name)
        if not os.path.isfile(path):
            continue

        if not is_ui_log(name) or not is_valid_format(name, valid_formats):
            continue

        if result.name is None:
            result = Result(name=name, path=path, date=get_date_from_log_name(name))
            continue

        current_date = get_date_from_log_name(name)
        if current_date > result.date:
            result = Result(name=name, path=path, date=current_date)

    if result.name is None:
        return empty_result

    return result


def parse_request_params(logfile):
    opener = gzip if logfile.name.endswith("gz") else open
    with opener.open(logfile.path, 'r') as file:
        for row in file:
            request = row.decode(encoding="utf-8")
            request_url = re.search(r"\"(GET|POST|PUT|HEAD|OPTIONS)\s\S+", request)
            if request_url is None:
                logging.warning(row)
                continue
            request_time = re.search(r"\s\d+\.\d+\s", request)
            if request_time is None:
                logging.warning(row)
            time = float(request_time.group())
            url = request_url.group().split()[-1]
            yield url, time


def get_table_json(logfile):
    data = {}
    count_total_req = 0
    request_time_sum = 0
    for url, time in parse_request_params(logfile):
        count_total_req += 1
        request_time_sum += time
        if url not in data:
            data[url] = {"count": 1, "time_sum": time, "time_max": time, "url": url}
        else:
            data_url = data[url]
            data_url["count"] += 1
            data_url["time_sum"] += time
            if time > data_url["time_max"]:
                data_url["time_max"] = time

    result = []
    for val in data.values():
        val["time_sum"] = round(val["time_sum"], ndigits=3)
        count_perc = (val["count"] / count_total_req) * 100
        val["count_perc"] = round(count_perc, ndigits=3)
        time_perc = (val["time_sum"] / request_time_sum) * 100
        val["time_perc"] = round(time_perc, ndigits=3)
        time_avg = val["time_sum"] / val["count"]
        val["time_avg"] = round(time_avg, ndigits=3)
        result.append(val)

    return result


def is_report_exist(date):
    return False


def render_template(table_json):
    with open("report.html", "r") as report:
        template = Template(report.read())
        content = template.safe_substitute(table_json=table_json)
    return content


def create_report_if_not_exist(content, logfile):
    if is_report_exist(logfile.name):
        return

    date_ = date.strftime(logfile.date, "%Y.%m.%d")
    name = f"report-{date_}.html"
    with open(name, "w") as report:
        report.write(content)


def main():
    parser = argparse.ArgumentParser(description="Анализатор логов nginx")
    parser.add_argument("--config", help="Путь к файлу с конфигурацией")

    logfile = get_latest_log_file(config["LOG_DIR"], config["VALID_LOG_FORMATS"])
    if logfile.name is None:
        logging.info("Не найдено файлов для анализа")
        return
    table_json = get_table_json(logfile)
    content = render_template(table_json)
    create_report_if_not_exist(content, logfile)
    render_template(table_json)


if __name__ == "__main__":
    main()
