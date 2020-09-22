#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

import argparse
import gzip
import json
import logging
import os
import re
import sys
import traceback
from collections import namedtuple
from datetime import date, datetime
from string import Template

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "OUTPUT_LOG_DIR": "./",
    "ERROR_LIMIT_PERC": 5,
}

LOG_FORMAT = "[%(asctime)s] %(levelname).1s %(message)s"
DATE_FMT = "%Y.%m.%d %H:%M:%S"

ERROR_EXIT_STATUS = 1
LogFile = namedtuple("LogFile", ["name", "path", "date"])

parser = argparse.ArgumentParser(description="Nginx logs analyzer")
parser.add_argument("--config", default="./config.json", help="Path to json config file")
args = parser.parse_args()
config_path = args.config


def get_date_from_log_name(log_name):
    """Вытаскивает дату создания из имени лога"""
    date_ = re.search(r"\d+", log_name).group()
    dt = datetime.strptime(date_, "%Y%m%d")
    return dt.date()


def get_latest_log_file(log_dir):
    """Просматривает папку с логами и находит самый свежий"""
    lf = LogFile(name=None, path=None, date=None)
    empty = lf
    if not os.path.exists(log_dir):
        return empty

    for name in os.listdir(log_dir):
        path = os.path.join(log_dir, name)
        if not os.path.isfile(path):
            continue

        match = re.match(r"^nginx-access-ui\.log-(?P<date>\d{8})(\.gz)?$", name)
        if match is None:
            continue

        if lf.name is None:
            lf = LogFile(name=name, path=path, date=get_date_from_log_name(name))
            continue

        current_date = get_date_from_log_name(name)
        if current_date > lf.date:
            lf = LogFile(name=name, path=path, date=current_date)

    if lf.name is None:
        return empty

    return lf


def get_opener(logname):
    """Объект для открытия файла лога"""
    return gzip.open if logname.endswith(".gz") else open


def request_params(logfile):
    """
    Генератор. На каждой итерации возвращает URL и время выполнения для каждой записи из файла лога
    """
    opener = get_opener(logfile.name)
    with opener(logfile.path, "r") as file:
        for row in file:
            url = time = None
            request = row.decode(encoding="utf-8")
            request_url = re.search(r"\"(GET|POST|PUT|HEAD|OPTIONS)\s\S+", request)
            if request_url is not None:
                request_time = re.search(r"\s\d+\.\d+\s", request)
                time = float(request_time.group())
                url = request_url.group().split()[-1]
            else:
                logging.info(f"Can not find url in request: {request.rstrip()}")
            yield url, time


def get_errors_limit(log_size, errors_limit_perc):
    """
    Возвращает максимально возможное количество ошибок.
    При превышении этого значения парсер остановит работу.
    """
    return int((log_size * errors_limit_perc) // 100)


def get_median(values):
    """Возвращает медиану для списка значений"""
    values.sort(reverse=True)
    len_ = len(values)
    if len_ % 2 == 0:
        mid_index_1 = len_ // 2
        mid_index_2 = mid_index_1 - 1
        return (values[mid_index_1] + values[mid_index_2]) / 2
    else:
        mid_index = len_ // 2
        return values[mid_index]


def get_statistics(logfile):
    """Возвращает статистику по запросам"""
    data = {}
    count_total_req = 0
    request_time_sum = 0
    total_rows_counter = 0
    errors_counter = 0
    for url, time in request_params(logfile):
        total_rows_counter += 1
        if url is None:
            errors_counter += 1
            continue
        count_total_req += 1
        request_time_sum += time
        if url not in data:
            data[url] = {
                "count": 1, "time_sum": time, "time_max": time, "url": url, "values": [time]
            }
        else:
            data_url = data[url]
            data_url["count"] += 1
            data_url["time_sum"] += time
            data_url["values"].append(time)
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
        values = val.pop("values")
        val["time_med"] = round(get_median(values), ndigits=3)
        result.append(val)

    return result, total_rows_counter, errors_counter


def is_report_exist(report_date, report_dir):
    """Проверяет, существует ли отчет за указанную дату"""
    expected_name = generate_report_name(report_date)
    path = os.path.join(report_dir, expected_name)
    if os.path.exists(path):
        return True
    return False


def render_template(table_json):
    """Создает шаблон отчета"""
    with open("report.html", "r") as report:
        template = Template(report.read())
        content = template.safe_substitute(table_json=table_json)
    return content


def generate_report_name(report_date):
    """Генерирует имя отчета для указанной даты"""
    date_ = date.strftime(report_date, "%Y.%m.%d")
    return f"report-{date_}.html"


def create_report(content, logfile, config):
    """Создает отчет"""
    report_name = generate_report_name(logfile.date)
    report_dir = config["REPORT_DIR"]
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    path = os.path.join(report_dir, report_name)
    with open(path, "w") as report:
        report.write(content)


def get_external_config(external_config_path):
    """Получить конфигурацию из внешнего файла"""
    with open(external_config_path, "r") as cf:
        cfg = json.load(cf)

    return cfg if isinstance(cfg, dict) else {}


def join_configs(basic_config, external_config):
    """Объединяет базовыю конфигурацию и конфигурацию из внешнего файла в один конфиг"""
    cfg = {}
    for key, value in basic_config.items():
        cfg[key] = value

    for key, value in external_config.items():
        cfg[key] = value

    return cfg


def main(basic_config, config_file_path):
    if not os.path.exists(config_file_path):
        logging.error("Config file not fount")
        sys.exit(ERROR_EXIT_STATUS)

    try:
        external_config = get_external_config(config_file_path)
    except json.decoder.JSONDecodeError:
        logging.error("Can not parse config file")
        sys.exit(ERROR_EXIT_STATUS)
    else:
        cfg = join_configs(basic_config, external_config)

    today = date.today()
    output_log_dir = cfg["OUTPUT_LOG_DIR"]
    filename = None
    if output_log_dir is not None:
        if not os.path.exists(output_log_dir):
            os.makedirs(output_log_dir)
        filename = os.path.join(output_log_dir, f"{today}.txt")

    logging.basicConfig(format=LOG_FORMAT, datefmt=DATE_FMT, filename=filename, level=logging.INFO)

    logfile = get_latest_log_file(cfg["LOG_DIR"])
    if logfile.name is None:
        logging.info("Nginx logs not found")
        return
    if is_report_exist(logfile.date, cfg["REPORT_DIR"]):
        logging.info(f"Report is already exists")
        return

    table_json, total_rows, errors_count = get_statistics(logfile)
    errors_limit = get_errors_limit(total_rows, config["ERROR_LIMIT_PERC"])
    if errors_count > errors_limit:
        logging.error("Can not create report. Too much errors.")
        sys.exit(ERROR_EXIT_STATUS)
    table_json.sort(key=lambda v: v["time_sum"], reverse=True)
    limit = cfg["REPORT_SIZE"]
    content = render_template(table_json[:limit])
    create_report(content, logfile, cfg)


if __name__ == "__main__":
    try:
        main(config, config_path)
    except Exception as exc:
        logging.exception(exc)
