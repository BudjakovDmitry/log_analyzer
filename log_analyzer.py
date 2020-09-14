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
from datetime import date
from string import Template

config = {
    "REPORT_SIZE": 1000,
    "VALID_LOG_FORMATS": ["gz", "log"],
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "OUTPUT_LOG_DIR": "./",
    "ERROR_LIMIT_PERC": 5,
}

LOG_FORMAT = "[%(asctime)s] %(levelname).1s %(message)s"
DATE_FMT = "%Y.%m.%d %H:%M:%S"
ERROR_EXIT_STATUS = 1

parser = argparse.ArgumentParser(description="Nginx logs analyzer")
parser.add_argument("--config", default="./config.json", help="Path to json config file")
args = parser.parse_args()
config_path = args.config


def exception_handler(type, value, tb):
    """Обработчик исключений"""
    tb_formatted = traceback.format_exception(type, value, tb)
    tb_str = "".join(tb_formatted)
    msg = f"{type.__name__}: {value}\n{tb_str}"
    logging.exception(msg, exc_info=False)


sys.excepthook = exception_handler


def is_ui_log(file_name):
    """Проверяет, что данный лог является логом интерфейса"""
    name = file_name.split(".")[0]
    type_ = name.split("-")[-1]
    return type_ == "ui"


def is_valid_format(file_name, valid_formats):
    """Проверяет, что лог имеет допустимое расширение"""
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


def get_opener(logname):
    """Объект для открытия файла лога"""
    return gzip if logname.endswith("gz") else open


def request_params(logfile, config):
    """
    Генератор. На каждой итерации возвращает URL и время выполнения для каждой записи из файла лога
    """
    opener = get_opener(logfile.name)
    log_size = get_log_size(logfile)
    errors_limit = get_errors_limit(log_size, config["ERROR_LIMIT_PERC"])
    errors_counter = 0
    with opener.open(logfile.path, "r") as file:
        for row in file:
            request = row.decode(encoding="utf-8")
            request_url = re.search(r"\"(GET|POST|PUT|HEAD|OPTIONS)\s\S+", request)
            if request_url is None:
                errors_counter += 1
                logging.info(f"Can not find url in request: {request.rstrip()}")
                if errors_counter >= errors_limit:
                    logging.error("Can not parse log file. Too much errors")
                    sys.exit(ERROR_EXIT_STATUS)
                continue

            request_time = re.search(r"\s\d+\.\d+\s", request)
            if request_time is None:
                errors_counter += 1
                logging.info(f"Can not find request time for request: {request.rstrip()}")
                if errors_counter >= errors_limit:
                    logging.error("Can not parse log file. Too much errors")
                    sys.exit(ERROR_EXIT_STATUS)
                continue

            time = float(request_time.group())
            url = request_url.group().split()[-1]
            yield url, time


def get_log_size(logfile):
    """Возвращает количество записей в лог-файле"""
    opener = get_opener(logfile.name)
    with opener.open(logfile.path, "rb") as file:
        counter = 0
        for _ in file:
            counter += 1
    return counter


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


def get_statistics(logfile, config):
    """Возвращает статистику по запросам"""
    data = {}
    count_total_req = 0
    request_time_sum = 0
    for url, time in request_params(logfile, config):
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

    return result


def is_report_exist(report_date, report_dir):
    """Проверяет, существует ли отчет за указанную дату"""
    if not os.path.exists(report_dir):
        return False
    expected_name = generate_report_name(report_date)
    for report in os.listdir(report_dir):
        if report == expected_name:
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


def main(basic_config, config_file_path):
    if not os.path.exists(config_file_path):
        logging.error("Config file not fount")
        sys.exit(ERROR_EXIT_STATUS)

    with open(config_file_path, "r") as cf:
        content = cf.read()
        if content:
            new_config_params = json.loads(content)
            basic_config.update(new_config_params)

    output_log_dir = basic_config.get("OUTPUT_LOG_DIR")
    filename = None
    today = date.today()
    if output_log_dir:
        if not os.path.exists(output_log_dir):
            os.makedirs(output_log_dir)
        filename = os.path.join(output_log_dir, f"{today}.txt")

    logging.basicConfig(format=LOG_FORMAT, datefmt=DATE_FMT, filename=filename, level=logging.INFO)

    logfile = get_latest_log_file(basic_config["LOG_DIR"], basic_config["VALID_LOG_FORMATS"])
    if logfile.name is None:
        logging.info("Nginx logs not found")
        return
    if is_report_exist(logfile.date, basic_config["REPORT_DIR"]):
        logging.info(f"Report is already exists")
        return

    table_json = get_statistics(logfile, basic_config)
    table_json.sort(key=lambda v: v["time_sum"], reverse=True)
    limit = basic_config["REPORT_SIZE"]
    content = render_template(table_json[:limit])
    create_report(content, logfile, basic_config)


if __name__ == "__main__":
    main(config, config_path)
