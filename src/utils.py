def get_last_version(table, column, spark, business_dt=date.today()):
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


def drop_old_partitions(table, version_column, leave_last_partitions, spark):
    parts = spark.sql(f"show partitions {table}")

    if (leave_last_partitions >= 0) & (parts.count() > leave_last_partitions):
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
                f.row_number().over(w.partitionBy("part").orderBy(f.col("dt").desc())),
            )
            .where(f.col("rn") > leave_last_partitions)
            .select("str_dt")
        ).collect()

        for curr_drop_part in list_dt_for_drop:
            sql_str = f"alter table {table} drop partition({version_column} = '{curr_drop_part[0]}') purge"  # noqa E501
            log.info(sql_str)
            spark.sql(sql_str).count()


def get_prev_table_from_ab_view(target_table_str: str, spark) -> str:
    try:
        old_param = (
            spark.sql(f"show create table {target_table_str}").select(
                f.regexp_extract(
                    f.col("createtab_stmt"), f"{target_table_str}_(a|b)", 1
                ).alias("param")
            )
        ).collect()[0][0]
    except Exception:
        old_param = "b"

    log.info(f"old_param = {old_param}")
    if old_param == "":
        prev_table = target_table_str
    else:
        prev_table = f"{target_table_str}_{old_param}"

    log.info(f"prev_table = {prev_table}")
