"""Tests for HDFS table size extrapolation to 2027."""

import unittest
from datetime import date

from hdfs_table_size import (
    BYTES_IN_GB,
    DAYS_IN_2027,
    YEAR_2027_END,
    YEAR_2027_START,
    calculate_hdfs_table_space_2027,
    estimate_daily_growth_bytes,
    monthly_sizes_2027,
    project_size_bytes,
)


class EstimateDailyGrowthTests(unittest.TestCase):
    def test_growth_from_year_start(self):
        # 215 GiB accumulated over 215 days => 1 GiB/day
        as_of = date(2026, 8, 4)  # day 216 of year; delta from Jan 1 = 215
        current = 215 * BYTES_IN_GB
        growth = estimate_daily_growth_bytes(current, as_of)
        self.assertAlmostEqual(growth, BYTES_IN_GB, places=3)

    def test_custom_baseline_and_window(self):
        growth = estimate_daily_growth_bytes(
            current_size_bytes=300 * BYTES_IN_GB,
            as_of_date=date(2026, 8, 4),
            observed_since=date(2026, 7, 5),
            baseline_size_bytes=270 * BYTES_IN_GB,
        )
        # 30 GiB over 30 days
        self.assertAlmostEqual(growth, BYTES_IN_GB, places=3)

    def test_rejects_zero_day_window(self):
        with self.assertRaises(ValueError):
            estimate_daily_growth_bytes(
                100,
                date(2026, 8, 4),
                observed_since=date(2026, 8, 4),
            )


class ProjectionTests(unittest.TestCase):
    def test_project_forward(self):
        size = project_size_bytes(
            current_size_bytes=100,
            as_of_date=date(2026, 8, 4),
            target_date=date(2026, 8, 14),
            daily_growth_bytes=10,
        )
        self.assertEqual(size, 200)

    def test_project_does_not_go_negative(self):
        size = project_size_bytes(
            current_size_bytes=10,
            as_of_date=date(2026, 8, 4),
            target_date=date(2026, 7, 1),
            daily_growth_bytes=0,
        )
        self.assertEqual(size, 10)


class Forecast2027Tests(unittest.TestCase):
    def test_full_year_2027_with_explicit_daily_growth(self):
        daily = BYTES_IN_GB  # 1 GiB/day
        forecast = calculate_hdfs_table_space_2027(
            current_size_bytes=100 * BYTES_IN_GB,
            as_of_date=date(2026, 8, 4),
            daily_growth_bytes=daily,
            replication_factor=3,
        )

        days_to_start = (YEAR_2027_START - date(2026, 8, 4)).days
        days_to_end = (YEAR_2027_END - date(2026, 8, 4)).days
        expected_start = 100 * BYTES_IN_GB + daily * days_to_start
        expected_end = 100 * BYTES_IN_GB + daily * days_to_end

        self.assertEqual(forecast.size_at_2027_start_bytes, expected_start)
        self.assertEqual(forecast.size_at_2027_end_bytes, expected_end)
        self.assertEqual(
            forecast.growth_during_2027_bytes,
            expected_end - expected_start,
        )
        # весь 2027: 364 интервала между 01.01 и 31.12, но прирост за 365 дней
        # от начала дня 01.01 до конца дня 31.12 = 364 дня разницы дат;
        # размер на дату — размер на этот календарный день, поэтому
        # growth = daily * (365 - 1) = daily * 364
        self.assertEqual(
            forecast.growth_during_2027_bytes,
            int(round(daily * (DAYS_IN_2027 - 1))),
        )
        self.assertEqual(
            forecast.ingest_full_year_2027_bytes,
            int(round(daily * DAYS_IN_2027)),
        )
        self.assertEqual(
            forecast.peak_physical_bytes_2027,
            expected_end * 3,
        )

    def test_monthly_series_covers_all_months(self):
        series = monthly_sizes_2027(
            current_size_bytes=BYTES_IN_GB,
            as_of_date=date(2026, 8, 4),
            daily_growth_bytes=BYTES_IN_GB,
        )
        self.assertEqual(len(series), 12)
        self.assertEqual(series[0][0], date(2027, 1, 31))
        self.assertEqual(series[-1][0], date(2027, 12, 31))
        sizes = [size for _, size in series]
        self.assertEqual(sizes, sorted(sizes))

    def test_from_current_consumption_default(self):
        # текущее потребление с начала 2026: 215 GiB к 4 августа
        forecast = calculate_hdfs_table_space_2027(
            current_size_bytes=215 * BYTES_IN_GB,
            as_of_date=date(2026, 8, 4),
        )
        self.assertAlmostEqual(forecast.daily_growth_bytes, BYTES_IN_GB, places=0)
        self.assertGreater(forecast.size_at_2027_end_bytes, forecast.size_at_2027_start_bytes)
        self.assertEqual(forecast.replication_factor, 3)


if __name__ == "__main__":
    unittest.main()
