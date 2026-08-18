"""Тесты логики A/B-хелпера с моком SparkSession."""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from common_utils.spark import get_prev_table_from_ab_view  # noqa: E402


def test_get_prev_table_from_ab_view_with_suffix():
    spark = MagicMock()
    spark.sql.return_value.select.return_value.collect.return_value = [("a",)]

    result = get_prev_table_from_ab_view("db.target", spark)
    assert result == "db.target_a"
    spark.sql.assert_called_once()


def test_get_prev_table_from_ab_view_empty_suffix():
    spark = MagicMock()
    spark.sql.return_value.select.return_value.collect.return_value = [("",)]

    result = get_prev_table_from_ab_view("db.target", spark)
    assert result == "db.target"


def test_get_prev_table_from_ab_view_fallback_on_error():
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("no such table")

    result = get_prev_table_from_ab_view("db.target", spark)
    assert result == "db.target_b"
