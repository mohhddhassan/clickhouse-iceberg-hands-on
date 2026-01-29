from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("iceberg-writer")
    # Iceberg Spark extensions
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    # Iceberg catalog config
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.type", "hadoop")
    .config("spark.sql.catalog.demo.warehouse", "s3a://lakehouse/warehouse")
    # S3 / MinIO config
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# Tiny dataset
data = [
    (1, "clickhouse", "2026-01-01"),
    (2, "iceberg", "2026-01-02"),
    (3, "spark", "2026-01-03"),
]

df = spark.createDataFrame(data, ["id", "tool", "dt"])

# Write Iceberg table (THIS creates Iceberg)
df.writeTo("demo.logs").using("iceberg").createOrReplace()

print("Iceberg table demo.logs written successfully")

spark.stop()
