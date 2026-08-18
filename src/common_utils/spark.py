"""Утилиты для работы с Hive/Spark-таблицами и партициями."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from pyspark.sql import functions as f
from pyspark.sql.window import Window

log = logging.getLogger(__name__)


def get_last_version(
    table: str,
    column: str,
    spark: Any,
    business_dt: date | None = None,
) -> date | None:
    """Вернуть максимальную дату партиции ``column`` не позже ``business_dt``.

    Ожидает формат партиций вида ``column=YYYY-MM-DD`` (возможно вложенный путь).
    """
    if business_dt is None:
        business_dt = date.today()

    lmda = lambda x: x.contains(column)  # noqa: E731
    actual_dt = (
        spark.sql(f"show partitions {table}")
        .select(
            f.split(f.filter(f.split(f.col("partition"), "/"), lmda)[0], "=")[1]
            .cast("date")
            .alias("column_dt")
        )
        .filter(f.col("column_dt") <= business_dt)
        .select(f.max(f.col("column_dt")))
        .collect()[0][0]
    )
    return actual_dt


def drop_old_partitions(
    table: str,
    version_column: str,
    leave_last_partitions: int,
    spark: Any,
) -> None:
    """Удалить старые date-партиции, оставив ``leave_last_partitions`` последних.

    Если ``leave_last_partitions < 0`` или партиций не больше лимита — ничего не делает.
    """
    parts = spark.sql(f"show partitions {table}")

    if (leave_last_partitions >= 0) and (parts.count() > leave_last_partitions):
        list_dt_for_drop = (
            parts.withColumn(
                "str_dt",
                f.regexp_extract(
                    f.col("partition"),
                    f"{version_column}=([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})",
                    1,
                ),
            )
            .withColumn("dt", f.col("str_dt").cast("date"))
            .select(f.col("str_dt"), f.col("dt"), f.lit(1).alias("part"))
            .distinct()
            .withColumn(
                "rn",
                f.row_number().over(
                    Window.partitionBy("part").orderBy(f.col("dt").desc())
                ),
            )
            .where(f.col("rn") > leave_last_partitions)
            .select("str_dt")
        ).collect()

        for curr_drop_part in list_dt_for_drop:
            sql_str = (
                f"alter table {table} drop partition("
                f"{version_column} = '{curr_drop_part[0]}') purge"
            )
            log.info(sql_str)
            spark.sql(sql_str).count()


def get_prev_table_from_ab_view(target_table_str: str, spark: Any) -> str:
    """Определить предыдущую физическую A/B-таблицу по DDL view.

    Ищет суффикс ``_a`` / ``_b`` в ``SHOW CREATE TABLE`` для ``target_table_str``.
    Если суффикс не найден — возвращает исходное имя таблицы.
    """
    try:
        old_param = (
            spark.sql(f"show create table {target_table_str}")
            .select(
                f.regexp_extract(
                    f.col("createtab_stmt"),
                    f"{target_table_str}_(a|b)",
                    1,
                ).alias("param")
            )
            .collect()[0][0]
        )
    except Exception:
        old_param = "b"

    log.info("old_param = %s", old_param)
    if old_param == "":
        prev_table = target_table_str
    else:
        prev_table = f"{target_table_str}_{old_param}"

    log.info("prev_table = %s", prev_table)
    return prev_table
