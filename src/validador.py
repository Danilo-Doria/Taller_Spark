from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("validador")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def leer(nombre):
    return (spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(f"datos/{nombre}.csv"))


customers = leer("customers")
products = leer("products")
orders = leer("orders")
items = leer("order_items")


def titulo(t):
    print(f"\n{'=' * 60}\n{t}\n{'=' * 60}")


# Verificar Nulos o vacíos por columna
def nulos(df, nombre):
    titulo(f"NULOS / VACIOS en {nombre}")
    df.select([
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1)
              .otherwise(0)).alias(c)
        for c in df.columns
    ]).show()


for df, n in [(customers, "customers"), (products, "products"),
              (orders, "orders"), (items, "order_items")]:
    nulos(df, n)


# Verificar claves duplicadas
def duplicados(df, clave, nombre):
    titulo(f"DUPLICADOS en {nombre}.{clave}")
    claves_dup = df.groupBy(clave).count().filter("count > 1").select(clave)
    df.join(claves_dup, clave, "inner").orderBy(clave).show(truncate=False)


duplicados(customers, "customer_id", "customers")
duplicados(products, "product_id", "products")
duplicados(orders, "order_id", "orders")
duplicados(items, "line_id", "order_items")

# Verificar dominios de valores
titulo("DOMINIOS")
orders.groupBy("status").count().orderBy("count").show()
orders.groupBy("channel").count().show()
customers.groupBy("country").count().show()
customers.groupBy("segment").count().show()
products.groupBy("category").count().show()
products.groupBy("active").count().show()

# Verificar rangos numéricos y valores mal formados
titulo("RANGOS en order_items")
items.select(
    F.min(F.col("quantity").cast("int")).alias("min_qty"),
    F.max(F.col("quantity").cast("int")).alias("max_qty"),
    F.min(F.col("discount_pct").cast("double")).alias("min_disc"),
    F.max(F.col("discount_pct").cast("double")).alias("max_disc"),
).show()

titulo("LINEAS con cantidad <= 0 o descuento fuera de [0,1]")
items.filter(
    (F.col("quantity").cast("int") <= 0)
    | (F.col("discount_pct").cast("double") < 0)
    | (F.col("discount_pct").cast("double") > 1)
    | F.col("quantity").cast("int").isNull()
    | F.col("discount_pct").cast("double").isNull()
).show(truncate=False)

titulo("PRODUCTOS con precio nulo o <= 0")
products.filter(
    F.col("unit_price").cast("double").isNull()
    | (F.col("unit_price").cast("double") <= 0)
).show(truncate=False)

titulo("FECHAS que no se pueden convertir a timestamp")
orders.filter(
    F.try_to_timestamp("order_ts").isNull()
    | F.try_to_timestamp("updated_at").isNull()
).show(truncate=False)

# Verificar referencias huerfanas
titulo("LINEAS sin pedido")
items.join(orders, "order_id", "left_anti").show(truncate=False)

titulo("LINEAS sin producto")
items.join(products, "product_id", "left_anti").show(truncate=False)

titulo("PEDIDOS sin cliente valido")
orders.join(customers, "customer_id", "left_anti").show(truncate=False)

spark.stop()
