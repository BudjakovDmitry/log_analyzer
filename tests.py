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


class TestDateFromLogname(unittest.TestCase):
    logname_gz = "nginx-access-ui.log-20170630.gz"

    def test_logname_gz(self):
        real_date = log_analyzer.get_date_from_logname(self.logname_gz)
        expected_date = datetime.date(year=2017, month=6, day=30)
        self.assertEqual(real_date, expected_date)


class TestLatestLogFile(unittest.TestCase):
    not_existing_dir = "/tmp/log_analyzer/not_existing"
    empty_log_dir = "/tmp/log_analyzer/empty"
    log_dir = "/tmp/log_analyzer/log_dir"

    @staticmethod
    def _get_random_datetime():
        start_date = datetime.date(1900, 1, 1)
        end_date = datetime.date(2050, 12, 31)
        delta = end_date - start_date
        delta_days = delta.days
        return start_date + datetime.timedelta(days=random.randrange(delta_days))

    @staticmethod
    def _get_log_date(filename):
        fn = filename.rstrip(".gz")
        date_str = fn.split("-")[-1]
        year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:])
        return datetime.date(year=year, month=month, day=day)

    def _create_ui_logs(self, count=10, log_format=None):
        if log_format is None:
            log_format = ".gz"
        for _ in range(count):
            date = self._get_random_datetime()
            month = f"{date.month}" if date.month >= 10 else f"0{date.month}"
            day = f"{date.day}" if date.day >= 10 else f"0{date.day}"
            name = f"nginx-access-ui.log-{date.year}{month}{day}{log_format}"
            path = os.path.join(self.log_dir, name)
            if log_format == ".gz":
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
        self._create_ui_logs()

    def test_log_dir_is_not_exist(self):
        log = log_analyzer.get_latest_log_file(self.not_existing_dir)
        self.assertIsInstance(log, tuple)
        self.assertIsNone(log.name)
        self.assertIsNone(log.path)
        self.assertIsNone(log.date)

    def test_log_dir_is_empty(self):
        log = log_analyzer.get_latest_log_file(self.empty_log_dir)
        self.assertIsInstance(log, tuple)
        self.assertIsNone(log.name)
        self.assertIsNone(log.path)
        self.assertIsNone(log.date)

    def test_return_format(self):
        log = log_analyzer.get_latest_log_file(self.log_dir)
        self.assertIsInstance(log, tuple, msg="Incorrect return type")

    def test_latest_log(self):
        files = []
        for _, _, filenames in os.walk(self.log_dir):
            files.extend(filenames)
        sorted_files = sorted(files, key=self._get_log_date, reverse=True)
        latest_log = sorted_files[0]
        Latest = namedtuple("Latest", ["name", "path", "date"])
        expected_latest = Latest(
            name=latest_log,
            path=os.path.join(self.log_dir, latest_log),
            date=self._get_log_date(latest_log)
        )
        real_latest = log_analyzer.get_latest_log_file(self.log_dir)
        self.assertEqual(expected_latest, real_latest, msg="Function return not latest log file")

    # 3 папка есть, содержимое есть, но нужных логов нет
    # 3 папка есть, и есть несколько логов подходящих и не подходящих


class TestMain(unittest.TestCase):
    def test_main(self):
        log_analyzer.main()


if __name__ == "__main__":
    unittest.main()
