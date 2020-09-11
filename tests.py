#!/usr/bin/env python
from collections import defaultdict, namedtuple
import datetime
import gzip
import shutil
import os
import random
import unittest
from unittest.mock import patch

import log_analyzer


def get_random_date():
    start_date = datetime.date(1900, 1, 1)
    end_date = datetime.date(2050, 12, 31)
    delta = end_date - start_date
    delta_days = delta.days
    return start_date + datetime.timedelta(days=random.randrange(delta_days))


class TestDefaultConfig(unittest.TestCase):
    def test_default_config_is_present(self):
        self.assertTrue(hasattr(log_analyzer, "config"), msg="Default config is not present")

    def test_config_format(self):
        config = log_analyzer.config
        self.assertIsInstance(config, dict, msg="Incorrect config format")

    def test_default_log_dir_is_present(self):
        config = log_analyzer.config
        self.assertIn("LOG_DIR", config, msg="'LOG_DIR' not in default config")


class TestValidLogFormat(unittest.TestCase):
    valid_log_formats = ["gz", "log"]
    name_gz = "some_log.log-20190305.gz"
    name_log = "some_log.log-20190630"
    name = "some_log"
    name_bz2 = "some_log.bz2"

    def test_gz(self):
        res = log_analyzer.is_valid_format(self.name_gz, self.valid_log_formats)
        self.assertTrue(res)

    def test_log(self):
        res = log_analyzer.is_valid_format(self.name_log, self.valid_log_formats)
        self.assertTrue(res)

    def test_empty(self):
        res = log_analyzer.is_valid_format(self.name, self.valid_log_formats)
        self.assertFalse(res)

    def test_bz2(self):
        res = log_analyzer.is_valid_format(self.name_bz2, self.valid_log_formats)
        self.assertFalse(res)


class TestIsUiLog(unittest.TestCase):
    ui_log = "nginx-access-ui.log-20190312"
    ui_gz = "nginx-access-ui.log-20200402.gz"
    not_ui_log = "nginx-access-other.log-20180301"
    not_ui_gz = "nginx-access-other.log-20200520.gz"

    def test_affirmative(self):
        res = log_analyzer.is_ui_log(self.ui_log)
        self.assertTrue(res)

    def test_affirmative_gz(self):
        res = log_analyzer.is_ui_log(self.ui_gz)
        self.assertTrue(res)

    def test_negative(self):
        res = log_analyzer.is_ui_log(self.not_ui_log)
        self.assertFalse(res)

    def test_negative_gz(self):
        res = log_analyzer.is_ui_log(self.not_ui_gz)
        self.assertFalse(res)


class TestDateFromLogname(unittest.TestCase):
    log_name_gz = "nginx-access-ui.log-20170630.gz"
    log_name_plain = "nginx-access-ui.log-20180421"

    def test_gz(self):
        real_date = log_analyzer.get_date_from_log_name(self.log_name_gz)
        expected_date = datetime.date(year=2017, month=6, day=30)
        self.assertEqual(real_date, expected_date)

    def test_plain(self):
        real_date = log_analyzer.get_date_from_log_name(self.log_name_plain)
        expected_date = datetime.date(year=2018, month=4, day=21)
        self.assertEqual(real_date, expected_date)


class TestLatestLogFile(unittest.TestCase):
    not_existing_dir = "/tmp/log_analyzer/not_existing"
    empty_log_dir = "/tmp/log_analyzer/empty"
    log_dir = "/tmp/log_analyzer/log_dir"

    valid_formats = ["gz", "log"]
    log_map = {}

    def _create_logs(self, count=10, log_type="ui", is_gz=False):
        for _ in range(count):
            date = get_random_date()
            month = f"{date.month}" if date.month >= 10 else f"0{date.month}"
            day = f"{date.day}" if date.day >= 10 else f"0{date.day}"
            name = f"nginx-access-{log_type}.log-{date.year}{month}{day}"
            if is_gz:
                name += ".gz"
            self.log_map[date] = name
            path = os.path.join(self.log_dir, name)
            if is_gz:
                with gzip.open(path, "wb") as f:
                    f.write(b"some content")
            else:
                with open(path, "w") as f:
                    f.write("some content")

    def setUp(self):
        for dir_ in self.not_existing_dir, self.empty_log_dir, self.log_dir:
            if os.path.exists(dir_):
                shutil.rmtree(dir_)

        for dir_ in self.empty_log_dir, self.log_dir:
            os.makedirs(dir_)

        self.log_map = {}

    def test_log_dir_is_not_exist(self):
        log = log_analyzer.get_latest_log_file(self.not_existing_dir, self.valid_formats)
        self.assertIsInstance(log, tuple)
        self.assertIsNone(log.name)
        self.assertIsNone(log.path)
        self.assertIsNone(log.date)

    def test_log_dir_is_empty(self):
        log = log_analyzer.get_latest_log_file(self.empty_log_dir, self.valid_formats)
        self.assertIsInstance(log, tuple)
        self.assertIsNone(log.name)
        self.assertIsNone(log.path)
        self.assertIsNone(log.date)

    def test_return_format(self):
        self._create_logs()
        log = log_analyzer.get_latest_log_file(self.log_dir, self.valid_formats)
        self.assertIsInstance(log, tuple, msg="Incorrect return type")

    def test_latest_log(self):
        self._create_logs()
        files = []
        for _, _, filenames in os.walk(self.log_dir):
            files.extend(filenames)
        sorted_dates = sorted(self.log_map.keys(), reverse=True)
        latest_date = sorted_dates[0]
        latest_log = self.log_map[latest_date]
        Latest = namedtuple("Latest", ["name", "path", "date"])
        expected_latest = Latest(
            name=latest_log,
            path=os.path.join(self.log_dir, latest_log),
            date=latest_date,
        )
        real_latest = log_analyzer.get_latest_log_file(self.log_dir, self.valid_formats)
        self.assertEqual(expected_latest, real_latest, msg="Function return not latest log file")


class TestGenerateReportName(unittest.TestCase):
    def test_less_than_ten(self):
        year = 2018
        month = 7
        day = 9
        date = datetime.date(year=year, month=month, day=day)
        expected_name = f"report-{year}.0{month}.0{day}.html"
        real_name = log_analyzer.generate_report_name(date)
        self.assertEqual(expected_name, real_name)

    def test_more_than_ten(self):
        year = 2018
        month = 12
        day = 31
        date = datetime.date(year=year, month=month, day=day)
        expected_name = f"report-{year}.{month}.{day}.html"
        real_name = log_analyzer.generate_report_name(date)
        self.assertEqual(expected_name, real_name)


class TestIsReportExist(unittest.TestCase):
    not_existing_report_dir = "/tmp/log_analyzer/not_existing_report_dir"
    existing_report_dir = "/tmp/log_analyzer/report_dir"

    def setUp(self):
        for dir_ in self.not_existing_report_dir, self.existing_report_dir:
            if os.path.exists(dir_):
                shutil.rmtree(dir_)

        os.makedirs(self.existing_report_dir)

    def _create_report(self, date):
        year = date.year
        month = date.month
        day = date.day
        month_str = str(month) if month > 10 else f"0{month}"
        day_str = str(day) if day > 10 else f"0{day}"
        name = f"report-{year}.{month_str}.{day_str}.html"
        path = os.path.join(self.existing_report_dir, name)
        with open(path, "wb") as report:
            report.write(b"")

    def test_report_exist(self):
        date = get_random_date()
        self._create_report(date)
        res = log_analyzer.is_report_exist(report_date=date, report_dir=self.existing_report_dir)
        self.assertTrue(res)

    def test_report_is_not_exist(self):
        date = get_random_date()
        res = log_analyzer.is_report_exist(report_date=date, report_dir=self.existing_report_dir)
        self.assertFalse(res)

    def test_dir_is_not_exist(self):
        date = get_random_date()
        res = log_analyzer.is_report_exist(
            report_date=date, report_dir=self.not_existing_report_dir
        )
        self.assertFalse(res)


class TestGetStatistic(unittest.TestCase):
    patched_method = "log_analyzer.request_params"
    mocked_requests = []
    route_counters = defaultdict(int)
    with open("./mocked_data/requests", "r") as requests:
        for row in requests:
            url, time = row.split()
            route_counters[url] += 1
            mocked_requests.append((url, float(time)))

    def test_split_by_url(self):
        with patch(self.patched_method, return_value=self.mocked_requests):
            result = log_analyzer.get_statistics(tuple())
        self.assertEqual(len(result), len(self.route_counters.keys()))

    def test_count(self):
        with patch(self.patched_method, return_value=self.mocked_requests):
            result = log_analyzer.get_statistics(tuple())
        for row in result:
            url = row["url"]
            expected_count = self.route_counters[url]
            real_count = row["count"]
            self.assertEqual(expected_count, real_count)


if __name__ == "__main__":
    unittest.main()
