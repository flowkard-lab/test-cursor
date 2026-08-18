# common-utils

Общая Python-библиотека утилит для переиспользования между репозиториями.
Управление зависимостями и сборка — через [Poetry](https://python-poetry.org/).

## Установка

### Из локального Artifactory

```bash
pip install common-utils --index-url https://artifactory.example.com/artifactory/api/pypi/pypi-local/simple
```

Или через Poetry в потребителе:

```toml
[[tool.poetry.source]]
name = "artifactory"
url = "https://artifactory.example.com/artifactory/api/pypi/pypi-local/simple"
priority = "supplemental"

[tool.poetry.dependencies]
common-utils = { version = "^0.1.0", source = "artifactory" }
```

### Локальная разработка

```bash
poetry install --extras spark
```

Без Spark (только каркас пакета):

```bash
poetry install
```

## Использование

```python
from common_utils import (
    get_last_version,
    drop_old_partitions,
    get_prev_table_from_ab_view,
)

actual_dt = get_last_version("db.table", "business_dt", spark)
drop_old_partitions("db.table", "business_dt", leave_last_partitions=7, spark=spark)
prev = get_prev_table_from_ab_view("db.target_view", spark)
```

## Публикация в Artifactory

1. Задайте URL репозитория (один раз на машине/CI):

```bash
poetry config repositories.artifactory \
  https://artifactory.example.com/artifactory/api/pypi/pypi-local
```

2. Аутентификация (токен или логин/пароль):

```bash
poetry config http-basic.artifactory <username> <password-or-token>
```

3. Сборка и публикация:

```bash
poetry build
poetry publish -r artifactory
```

Версию поднимайте в `pyproject.toml` (`[tool.poetry].version`) и в `src/common_utils/__init__.py` (`__version__`).

## Структура

```
src/common_utils/     # пакет
  __init__.py         # публичный API
  spark.py            # Hive/Spark-утилиты
tests/                # тесты
pyproject.toml        # Poetry / метаданные пакета
```

## Разработка

```bash
poetry install --extras spark --with dev
poetry run pytest
poetry build
```
