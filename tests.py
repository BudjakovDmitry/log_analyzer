#!/usr/bin/env python
from collections import namedtuple
import datetime
import gzip
import shutil
import os
import random
import unittest

import log_analyzer


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
    name_gz = "some_log.gz"
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
    not_ui_log = "nginx-access-blabla.log-20180301"

    def test_affirmative(self):
        res = log_analyzer.is_ui_log(self.ui_log)
        self.assertTrue(res)

    def test_negative(self):
        res = log_analyzer.is_ui_log(self.not_ui_log)
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

    valid_formats = ["gz", ".og"]
    log_map = {}

    @staticmethod
    def _get_random_datetime():
        start_date = datetime.date(1900, 1, 1)
        end_date = datetime.date(2050, 12, 31)
        delta = end_date - start_date
        delta_days = delta.days
        return start_date + datetime.timedelta(days=random.randrange(delta_days))

    # @staticmethod
    # def _get_log_date(filename):
    #     fn = filename.rstrip(".gz")
    #     date_str = fn.split("-")[-1]
    #     year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:])
    #     return datetime.date(year=year, month=month, day=day)

    def _create_logs(self, count=10, log_type="ui", log_format=None):
        for _ in range(count):
            date = self._get_random_datetime()
            month = f"{date.month}" if date.month >= 10 else f"0{date.month}"
            day = f"{date.day}" if date.day >= 10 else f"0{date.day}"
            name = f"nginx-access-{log_type}.log-{date.year}{month}{day}{log_format}"
            self.log_map[date] = name
            path = os.path.join(self.log_dir, name)
            if log_format == "gz":
                with gzip.open(path, "wb") as f:
                    f.write(b"some content")
            else:
                with open(path, "w") as f:
                    f.write("some content")

    def setUp(self):
        for dir_ in self.not_existing_dir, self.empty_log_dir, self.log_dir:
            if os.path.exists(dir_):
                shutil.rmtree(dir_)

        os.makedirs(self.empty_log_dir)
        os.makedirs(self.log_dir)
        self.log_map = {}
        self._create_logs()

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
        log = log_analyzer.get_latest_log_file(self.log_dir, self.valid_formats)
        self.assertIsInstance(log, tuple, msg="Incorrect return type")

    def test_latest_log(self):
        files = []
        for _, _, filenames in os.walk(self.log_dir):
            files.extend(filenames)
        sorted_dates = sorted(self.log_map.keys())
        latest_date = sorted_dates[0]
        latest_log = self.log_map[latest_date]
        Latest = namedtuple("Latest", ["name", "path", "date"])
        expected_latest = Latest(
            name=latest_log,
            path=os.path.join(self.log_dir, latest_log),
            date=latest_date,
        )
        real_latest = log_analyzer.get_latest_log_file(self.log_dir, self)
        self.assertEqual(expected_latest, real_latest, msg="Function return not latest log file")

    # 3 папка есть, содержимое есть, но нужных логов нет
    # 3 папка есть, и есть несколько логов подходящих и не подходящих


class TestMain(unittest.TestCase):
    def test_main(self):
        pass
        # log_analyzer.main()


if __name__ == "__main__":
    unittest.main()
