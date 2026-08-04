"""Расчёт и экстраполяция размера таблицы в HDFS на 2027 год."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable


BYTES_IN_GB = 1024**3
BYTES_IN_TB = 1024**4
YEAR_2027_START = date(2027, 1, 1)
YEAR_2027_END = date(2027, 12, 31)
DAYS_IN_2027 = (YEAR_2027_END - YEAR_2027_START).days + 1  # 365


@dataclass(frozen=True)
class HdfsTableForecast:
    """Прогноз места, занимаемого таблицей в HDFS."""

    current_size_bytes: int
    as_of_date: date
    daily_growth_bytes: float
    replication_factor: int
    size_at_2027_start_bytes: int
    size_at_2027_end_bytes: int
    growth_during_2027_bytes: int
    ingest_full_year_2027_bytes: int
    peak_logical_bytes_2027: int
    peak_physical_bytes_2027: int

    @property
    def size_at_2027_start_gb(self) -> float:
        return self.size_at_2027_start_bytes / BYTES_IN_GB

    @property
    def size_at_2027_end_gb(self) -> float:
        return self.size_at_2027_end_bytes / BYTES_IN_GB

    @property
    def growth_during_2027_gb(self) -> float:
        return self.growth_during_2027_bytes / BYTES_IN_GB

    @property
    def ingest_full_year_2027_gb(self) -> float:
        return self.ingest_full_year_2027_bytes / BYTES_IN_GB

    @property
    def peak_logical_tb_2027(self) -> float:
        return self.peak_logical_bytes_2027 / BYTES_IN_TB

    @property
    def peak_physical_tb_2027(self) -> float:
        return self.peak_physical_bytes_2027 / BYTES_IN_TB


def _clamp_non_negative(value: float) -> float:
    return max(0.0, value)


def _to_int_bytes(value: float) -> int:
    return int(round(_clamp_non_negative(value)))


def estimate_daily_growth_bytes(
    current_size_bytes: int,
    as_of_date: date,
    *,
    observed_since: date | None = None,
    baseline_size_bytes: int = 0,
) -> float:
    """Оценивает среднесуточный прирост по текущему потреблению.

    Если `observed_since` не задан, берётся начало текущего года
    (`as_of_date`), а `baseline_size_bytes` — размер на эту дату
    (по умолчанию 0: всё текущее потребление накоплено за период).
    """
    if current_size_bytes < 0:
        raise ValueError("current_size_bytes must be >= 0")
    if baseline_size_bytes < 0:
        raise ValueError("baseline_size_bytes must be >= 0")
    if baseline_size_bytes > current_size_bytes:
        raise ValueError("baseline_size_bytes cannot exceed current_size_bytes")

    start = observed_since or date(as_of_date.year, 1, 1)
    if start > as_of_date:
        raise ValueError("observed_since cannot be after as_of_date")

    elapsed_days = (as_of_date - start).days
    if elapsed_days <= 0:
        raise ValueError(
            "need at least 1 full day of observation to estimate growth "
            f"(as_of_date={as_of_date}, observed_since={start})"
        )

    grown = current_size_bytes - baseline_size_bytes
    return grown / elapsed_days


def project_size_bytes(
    current_size_bytes: int,
    as_of_date: date,
    target_date: date,
    daily_growth_bytes: float,
) -> int:
    """Линейная экстраполяция логического размера таблицы на дату."""
    if current_size_bytes < 0:
        raise ValueError("current_size_bytes must be >= 0")
    delta_days = (target_date - as_of_date).days
    projected = current_size_bytes + daily_growth_bytes * delta_days
    return _to_int_bytes(projected)


def monthly_sizes_2027(
    current_size_bytes: int,
    as_of_date: date,
    daily_growth_bytes: float,
) -> list[tuple[date, int]]:
    """Размер таблицы на конец каждого месяца 2027 года."""
    month_ends = [
        date(2027, 1, 31),
        date(2027, 2, 28),
        date(2027, 3, 31),
        date(2027, 4, 30),
        date(2027, 5, 31),
        date(2027, 6, 30),
        date(2027, 7, 31),
        date(2027, 8, 31),
        date(2027, 9, 30),
        date(2027, 10, 31),
        date(2027, 11, 30),
        date(2027, 12, 31),
    ]
    return [
        (
            month_end,
            project_size_bytes(
                current_size_bytes, as_of_date, month_end, daily_growth_bytes
            ),
        )
        for month_end in month_ends
    ]


def iter_daily_sizes_2027(
    current_size_bytes: int,
    as_of_date: date,
    daily_growth_bytes: float,
) -> Iterable[tuple[date, int]]:
    """Итератор дневных размеров таблицы на весь 2027 год."""
    day = YEAR_2027_START
    while day <= YEAR_2027_END:
        yield (
            day,
            project_size_bytes(
                current_size_bytes, as_of_date, day, daily_growth_bytes
            ),
        )
        day += timedelta(days=1)


def calculate_hdfs_table_space_2027(
    current_size_bytes: int,
    *,
    as_of_date: date | None = None,
    observed_since: date | None = None,
    baseline_size_bytes: int = 0,
    daily_growth_bytes: float | None = None,
    replication_factor: int = 3,
) -> HdfsTableForecast:
    """Считает место таблицы в HDFS и экстраполирует на весь 2027 год.

    Параметры
    ---------
    current_size_bytes:
        Текущий логический размер таблицы (без учёта репликации), байты.
    as_of_date:
        Дата замера текущего размера. По умолчанию — сегодня.
    observed_since / baseline_size_bytes:
        Период и размер, по которым оценивается текущее потребление
        (среднесуточный прирост), если `daily_growth_bytes` не задан.
        По умолчанию: с 1 января текущего года, baseline = 0.
    daily_growth_bytes:
        Явный среднесуточный прирост (логический), байты/день.
    replication_factor:
        Коэффициент репликации HDFS для физического объёма (обычно 3).

    Возвращает прогноз на начало/конец 2027, прирост за год и пиковый
    логический/физический объём к 31.12.2027.
    """
    if replication_factor < 1:
        raise ValueError("replication_factor must be >= 1")

    measured_on = as_of_date or date.today()
    growth = (
        daily_growth_bytes
        if daily_growth_bytes is not None
        else estimate_daily_growth_bytes(
            current_size_bytes,
            measured_on,
            observed_since=observed_since,
            baseline_size_bytes=baseline_size_bytes,
        )
    )
    if growth < 0:
        raise ValueError("daily_growth_bytes must be >= 0")

    size_start = project_size_bytes(
        current_size_bytes, measured_on, YEAR_2027_START, growth
    )
    size_end = project_size_bytes(
        current_size_bytes, measured_on, YEAR_2027_END, growth
    )
    # Разница размеров на 01.01 и 31.12 (364 суточных шага).
    growth_2027 = max(0, size_end - size_start)
    # Полный объём прироста за все 365 дней 2027 года.
    ingest_full_year = _to_int_bytes(growth * DAYS_IN_2027)
    peak_logical = size_end
    peak_physical = peak_logical * replication_factor

    return HdfsTableForecast(
        current_size_bytes=current_size_bytes,
        as_of_date=measured_on,
        daily_growth_bytes=growth,
        replication_factor=replication_factor,
        size_at_2027_start_bytes=size_start,
        size_at_2027_end_bytes=size_end,
        growth_during_2027_bytes=growth_2027,
        ingest_full_year_2027_bytes=ingest_full_year,
        peak_logical_bytes_2027=peak_logical,
        peak_physical_bytes_2027=peak_physical,
    )


def format_forecast(forecast: HdfsTableForecast) -> str:
    """Человекочитаемый отчёт по прогнозу на 2027 год."""
    return "\n".join(
        [
            "HDFS table size forecast for 2027",
            f"  as of:                 {forecast.as_of_date.isoformat()}",
            f"  current logical size:  {forecast.current_size_bytes / BYTES_IN_GB:.3f} GiB",
            f"  daily growth:          {forecast.daily_growth_bytes / BYTES_IN_GB:.6f} GiB/day",
            f"  replication factor:    {forecast.replication_factor}",
            f"  size at 2027-01-01:    {forecast.size_at_2027_start_gb:.3f} GiB",
            f"  size at 2027-12-31:    {forecast.size_at_2027_end_gb:.3f} GiB",
            f"  delta 01.01→31.12:     {forecast.growth_during_2027_gb:.3f} GiB",
            f"  full-year ingest 2027: {forecast.ingest_full_year_2027_gb:.3f} GiB "
            f"({DAYS_IN_2027} days)",
            f"  peak logical 2027:     {forecast.peak_logical_tb_2027:.6f} TiB",
            f"  peak physical 2027:    {forecast.peak_physical_tb_2027:.6f} TiB",
        ]
    )


if __name__ == "__main__":
    # Пример: таблица 1.5 TiB на 2026-08-04, рост оценён с начала 2026.
    example = calculate_hdfs_table_space_2027(
        current_size_bytes=int(1.5 * BYTES_IN_TB),
        as_of_date=date(2026, 8, 4),
        replication_factor=3,
    )
    print(format_forecast(example))
    print("\nMonth-end sizes (GiB):")
    for month_end, size in monthly_sizes_2027(
        example.current_size_bytes,
        example.as_of_date,
        example.daily_growth_bytes,
    ):
        print(f"  {month_end.isoformat()}: {size / BYTES_IN_GB:.3f}")
