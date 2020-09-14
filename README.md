# Log Analyzer

## Что это?
Утилита для парсинга логов Nginx и получения статистики о времени выполнения запросов.

## Как с этим работать?
Скачиваем утилиту

```
git clone git@github.com:BudjakovDmitry/log_analyzer.git
cd log_analyzer
```

### Запуск тестов
```
python3 tests.py
```

### Запуск анализатора логов
```
python3 log_analyzer.py
```

## Описание
Программа ищет логи в указанной папке, достает самый свежий, и строит отчет в формате html.

По умолчанию логи ищутся в директории "./log", а готовые отчеты складываются в директорию
 "./reports" (как их изменить, см. ниже). Помимо отчета, программа оставляет лог своей работы,
 который по умолчанию пишется в текущую директорию в формате "YYYY-MM-DD.txt"

### Конфигурация
Поведение программы можно изменить, передав при запуске файл конфигурации в формате json через ключ
 --config
```
python3 log_analyzer.py --config path/to/config.json
```
Если ключ --config не передан, то файл конфигурации будет искаться в текущей директории.

Парамерты конфигурации. В скобках указаны значения по умолчанию.
* **REPORT_SIZE** - колчество URL, которые будут показаны в отчете (1 000)
* **REPORT_DIR** - папка, куда будут складываться текущие отчеты (./reports)
* **LOG_DIR** - папка, в которой ищутся логи для обработки (./log)
* **OUTPUT_LOG_DIR** - папка, в которую складываются логи работы программы (текущая директория)
* **ERROR_LIMIT_PERC** - пороговое значение ошибок парсинга в процентах (5%). Отношение запросов,
 которые не удается распарсить к общему количеству запросов в лог-файле. При превышении этого
 значения программа завершает работу с ошибкой.
