# test-cursor

Расчёт места, занимаемого таблицей в HDFS, и линейная экстраполяция на весь 2027 год.

## Использование

```python
from datetime import date
from hdfs_table_size import calculate_hdfs_table_space_2027, format_forecast

# Текущий логический размер таблицы (байты), дата замера.
# Среднесуточный прирост оценивается по потреблению с 1 января текущего года.
forecast = calculate_hdfs_table_space_2027(
    current_size_bytes=1_648_849_920_000,  # пример
    as_of_date=date(2026, 8, 4),
    replication_factor=3,
)

print(format_forecast(forecast))
print(forecast.size_at_2027_end_bytes)      # размер на 31.12.2027
print(forecast.peak_physical_bytes_2027)    # с учётом репликации
print(forecast.ingest_full_year_2027_bytes) # прирост за все 365 дней 2027
```

Явный дневной прирост:

```python
forecast = calculate_hdfs_table_space_2027(
    current_size_bytes=1_648_849_920_000,
    as_of_date=date(2026, 8, 4),
    daily_growth_bytes=5_000_000_000,  # ~5 GB/день
)
```

## Запуск

```bash
python3 hdfs_table_size.py
python3 -m unittest test_hdfs_table_size.py -v
```
