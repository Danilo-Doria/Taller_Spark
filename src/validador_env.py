from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("check")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("Spark version:", spark.version)

for nombre in ["customers", "products", "orders", "order_items"]:
    df = spark.read.option("header", True).csv(f"datos/{nombre}.csv")
    print(f"{nombre}: {df.count()} filas, {len(df.columns)} columnas")

for nombre in ["orders_incremental", "order_items_incremental"]:
    df = spark.read.json(f"datos/incremental/{nombre}.jsonl")
    print(f"{nombre}: {df.count()} filas")

spark.stop()
