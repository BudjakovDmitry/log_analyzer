#!/usr/bin/env python
from collections import defaultdict, namedtuple
import datetime
import gzip
import shutil
import os
import random
import unittest
import json
from unittest.mock import patch

import log_analyzer


TEST_CONFIG = {"key1": "value1", "key2": "value2", "key3": "value3"}
Log = namedtuple("Log", ["name", "path", "date"])
empty_log = Log(name=None, path=None, date=None)

OK_EXIT_CODE = 0


def get_random_date():
    start_date = datetime.date(1900, 1, 1)
    end_date = datetime.date(2050, 12, 31)
    delta = end_date - start_date
    delta_days = delta.days
    return start_date + datetime.timedelta(days=random.randrange(delta_days))


def remove_dirs(*dirs):
    for dir_ in dirs:
        if os.path.exists(dir_):
            shutil.rmtree(dir_)


class TestDefaultConfig(unittest.TestCase):
    def test_default_config_is_present(self):
        self.assertTrue(hasattr(log_analyzer, "config"), msg="Default config is not present")

    def test_config_format(self):
        config = log_analyzer.config
        self.assertIsInstance(config, dict, msg="Incorrect config format")

    def test_default_params(self):
        config = log_analyzer.config
        self.assertIn("REPORT_SIZE", config, msg="'REPORT_SIZE' not in default config")
        self.assertIn("REPORT_DIR", config, msg="'REPORT_DIR' not in default config")
        self.assertIn("LOG_DIR", config, msg="'LOG_DIR' not in default config")
        self.assertIn("OUTPUT_LOG_DIR", config, msg="'OUTPUT_LOG_DIR' not in default config")
        self.assertIn("ERROR_LIMIT_PERC", config, msg="'ERROR_LIMIT_PERC' not in default config")


class TestExternalConfig(unittest.TestCase):
    default_config_file = "./config.json"
    config_dir = "/tmp/log_analyzer/test_external_config"

    def setUp(self):
        remove_dirs(self.config_dir)
        os.makedirs(self.config_dir)

    def test_config_in_args(self):
        args = log_analyzer.args
        self.assertTrue(hasattr(args, "config"))

    def test_config_default(self):
        config_file = log_analyzer.args.config
        self.assertEqual(config_file, self.default_config_file)

    def test_conf_not_found(self):
        """Если файла конфигурации не существует, то падает с ошибкой"""
        config_file = os.path.join(self.config_dir, "not_existing_config.json")
        with self.assertRaises(SystemExit) as sys_exit:
            log_analyzer.main(TEST_CONFIG, config_file)
        self.assertNotEqual(sys_exit.exception.code, OK_EXIT_CODE)

    def test_config_is_not_valid(self):
        """Если файл конфигурации не парсится, то падает с ошибкой"""
        cfg_path = os.path.join(self.config_dir, "cfg.json")
        with open(cfg_path, "w") as cfg:
            cfg.write("invalid config format")

        with self.assertRaises(SystemExit) as sys_exit:
            log_analyzer.main(TEST_CONFIG, cfg_path)
        self.assertNotEqual(sys_exit.exception.code, OK_EXIT_CODE)

    def test_join_config(self):
        first = {"key1": "value1", "key2": "value2"}
        second = {"key1": "value100", "key3": "value3"}
        summary_config = log_analyzer.join_configs(first, second)
        self.assertDictEqual(
            summary_config, {"key1": "value100", "key2": "value2", "key3": "value3"}
        )


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
    not_existing_dir = "/tmp/log_analyzer/test_latest_log_file/not_existing"
    empty_log_dir = "/tmp/log_analyzer/test_latest_log_file/empty"
    log_dir = "/tmp/log_analyzer/test_latest_log_file/log_dir"

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
        remove_dirs(self.not_existing_dir, self.empty_log_dir, self.log_dir)

        for dir_ in self.empty_log_dir, self.log_dir:
            os.makedirs(dir_)

        self.log_map = {}

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
        self._create_logs()
        log = log_analyzer.get_latest_log_file(self.log_dir)
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
        real_latest = log_analyzer.get_latest_log_file(self.log_dir)
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
    not_existing_report_dir = "/tmp/log_analyzer/test_is_report_exist/not_existing_report_dir"
    existing_report_dir = "/tmp/log_analyzer/test_is_report_exist/report_dir"

    def setUp(self):
        remove_dirs(self.not_existing_report_dir, self.existing_report_dir)
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
    request_params = "log_analyzer.request_params"
    assertion_delta = 1e-5
    mocked_requests = []
    route_counters = defaultdict(int)
    time_sum = defaultdict(int)
    time_values = defaultdict(list)
    total_requests_count = 0
    total_rows_count = 0
    errors_count = 0
    total_time_sum = 0

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        if cls.mocked_requests:
            return obj

        with open("./mocked_data/requests", "r") as requests:
            for row in requests:
                cls.total_rows_count += 1
                if row == "\n":
                    cls.errors_count += 1
                    cls.mocked_requests.append((None, None))
                    continue
                url, time = row.split()
                time = float(time)
                cls.route_counters[url] += 1
                cls.time_sum[url] += time
                cls.total_requests_count += 1
                cls.total_time_sum += time
                cls.time_values[url].append(time)
                cls.mocked_requests.append((url, time))

        return obj

    def test_split_by_url(self):
        with patch(self.request_params, return_value=self.mocked_requests):
            result, *_ = log_analyzer.get_statistics(tuple())
        self.assertEqual(len(result), len(self.route_counters.keys()))

    def test_count(self):
        with patch(self.request_params, return_value=self.mocked_requests):
            result, *_ = log_analyzer.get_statistics(tuple())
        for row in result:
            url = row["url"]
            expected_count = self.route_counters[url]
            real_count = row["count"]
            self.assertEqual(expected_count, real_count)

    def test_count_perc(self):
        with patch(self.request_params, return_value=self.mocked_requests):
            result, *_ = log_analyzer.get_statistics(tuple())
        for row in result:
            url = row["url"]
            expected_perc = (self.route_counters[url] / self.total_requests_count) * 100
            expected_perc = round(expected_perc, ndigits=3)
            real_perc = row["count_perc"]
            self.assertEqual(expected_perc, real_perc)

    def test_time_sum(self):
        with patch(self.request_params, return_value=self.mocked_requests):
            result, *_ = log_analyzer.get_statistics(tuple())
        for row in result:
            url = row["url"]
            expected_time_sum = self.time_sum[url]
            real_time_sum = row["time_sum"]
            delta = abs(expected_time_sum - real_time_sum)
            self.assertLess(delta, self.assertion_delta)

    def test_time_perc(self):
        with patch(self.request_params, return_value=self.mocked_requests):
            result, *_ = log_analyzer.get_statistics(tuple())
        for row in result:
            url = row["url"]
            expected_time_perc = (self.time_sum[url] / self.total_time_sum) * 100
            expected_time_perc = round(expected_time_perc, ndigits=3)
            real_time_perc = row["time_perc"]
            delta = abs(expected_time_perc - real_time_perc)
            self.assertLess(delta, self.assertion_delta)

    def test_time_avg(self):
        with patch(self.request_params, return_value=self.mocked_requests):
            result, *_ = log_analyzer.get_statistics(tuple())
        for row in result:
            url = row["url"]
            values = self.time_values[url]
            expected_avg = round(sum(values) / len(values), ndigits=3)
            real_avg = row["time_avg"]
            delta = abs(expected_avg - real_avg)
            self.assertLess(delta, self.assertion_delta)

    def test_time_max(self):
        with patch(self.request_params, return_value=self.mocked_requests):
            result, *_ = log_analyzer.get_statistics(tuple())
        for row in result:
            url = row["url"]
            values = self.time_values[url]
            expected_max_value = max(values)
            real_max_value = row["time_max"]
            delta = abs(expected_max_value - real_max_value)
            self.assertLess(delta, self.assertion_delta)

    def test_time_med(self):
        with patch(self.request_params, return_value=self.mocked_requests):
            result, *_ = log_analyzer.get_statistics(tuple())
        for row in result:
            url = row["url"]
            values = self.time_values[url]
            values.sort(reverse=True)
            len_values = len(values)
            central_index = len_values // 2
            if len_values % 2 != 0:
                expected_med = values[central_index]
            else:
                val_1 = values[central_index]
                val_2 = values[central_index - 1]
                expected_med = (val_1 + val_2) / 2
            real_med = row["time_med"]
            delta = abs(expected_med - real_med)
            self.assertLess(delta, self.assertion_delta)

    def test_total_rows_counter(self):
        with patch(self.request_params, return_value=self.mocked_requests):
            _, total_rows, _ = log_analyzer.get_statistics(tuple())
        self.assertEqual(self.total_rows_count, total_rows)

    def test_errors_counter(self):
        with patch(self.request_params, return_value=self.mocked_requests):
            *_, errors = log_analyzer.get_statistics(tuple())
        self.assertEqual(self.errors_count, errors)


class TestGetMediane(unittest.TestCase):
    assertion_delta = 1e-5

    def test_one_element(self):
        one_element = [random.randint(0, 100)]
        expected_med = one_element[0]
        real_med = log_analyzer.get_median(one_element)
        self.assertEqual(expected_med, real_med)

    def test_two_elements(self):
        two_elements = [random.randint(0, 100) for _ in range(2)]
        expected_med = sum(two_elements) / len(two_elements)
        real_med = log_analyzer.get_median(two_elements)
        delta = abs(expected_med - real_med)
        self.assertLess(delta, self.assertion_delta)

    def test_odd(self):
        odd = [random.randint(0, 100) for _ in range(55)]
        odd.sort(reverse=True)
        index = len(odd) // 2
        expected_med = odd[index]
        real_med = log_analyzer.get_median(odd)
        self.assertEqual(expected_med, real_med)

    def test_even(self):
        even = [random.randint(0, 100) for _ in range(42)]
        even.sort(reverse=True)
        second = len(even) // 2
        first = second - 1
        expected_med = (even[second] + even[first]) / 2
        real_med = log_analyzer.get_median(even)
        delta = abs(expected_med - real_med)
        self.assertLess(delta, self.assertion_delta)


class TestOpener(unittest.TestCase):
    def test_gz(self):
        opener = log_analyzer.get_opener("file.gz")
        self.assertEqual(opener, gzip.open)

    def test_open(self):
        opener = log_analyzer.get_opener("file.log")
        self.assertEqual(opener, open)


class TestGetExternalConfig(unittest.TestCase):
    dir = "/tmp/log_analyzer/external_config"
    valid = "valid.json"
    invalid = "invalid.json"

    def setUp(self):
        remove_dirs(self.dir)
        os.makedirs(self.dir)

    def write_file(self, filename, content):
        path = os.path.join(self.dir, filename)
        with open(path, "w") as cfg:
            cfg.write(content)

    def test_valid_config(self):
        expected = {"key1": "value1", "key2": "value2", "key3": "value3"}
        content = json.dumps(expected)
        self.write_file(self.valid, content)
        path = os.path.join(self.dir, self.valid)
        real = log_analyzer.get_external_config(path)
        self.assertDictEqual(real, expected)

    def test_decode_error(self):
        content = "Wrong format"
        self.write_file(self.invalid, content)
        path = os.path.join(self.dir, self.invalid)
        with self.assertRaises(json.decoder.JSONDecodeError):
            log_analyzer.get_external_config(path)

    def test_not_json(self):
        content = str(42)
        self.write_file(self.invalid, content)
        path = os.path.join(self.dir, self.invalid)
        real = log_analyzer.get_external_config(path)
        self.assertDictEqual(real, {})

    def test_empty_config(self):
        content = ""
        self.write_file(self.valid, content)
        path = os.path.join(self.dir, self.valid)
        with self.assertRaises(json.decoder.JSONDecodeError):
            log_analyzer.get_external_config(path)


class TestMain(unittest.TestCase):
    def test_error_exit_code(self):
        self.assertNotEqual(log_analyzer.ERROR_EXIT_STATUS, OK_EXIT_CODE)


if __name__ == "__main__":
    unittest.main()
