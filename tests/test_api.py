"""Базовые тесты публичного API пакета (без Spark-кластера)."""

import common_utils


def test_version_is_semver_like():
    parts = common_utils.__version__.split(".")
    assert len(parts) >= 2
    assert all(p.isdigit() for p in parts[:2])


def test_public_api_exports_listed():
    for name in (
        "get_last_version",
        "drop_old_partitions",
        "get_prev_table_from_ab_view",
    ):
        assert name in common_utils.__all__
