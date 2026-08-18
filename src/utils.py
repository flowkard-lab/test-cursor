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
