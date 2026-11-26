from pyspark.sql import SparkSession


def session(settings):
    spark = (
        SparkSession.builder.appName("MedLens")
        .master(f"local[{settings.cores}]")
        .config("spark.driver.memory", f"{settings.memory_gb}g")
        .config("spark.sql.shuffle.partitions", settings.partitions)
        .config("spark.sql.session.timeZone", settings.timezone)
        .config("spark.sql.caseSensitive", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
