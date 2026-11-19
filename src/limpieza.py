import sys

from pyspark.sql import SparkSession, Window, functions as F

INCREMENTAL = "--sin-incremental" not in sys.argv
DUPLICAR_LOTE = "--duplicar-lote" in sys.argv

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("limpieza")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

ESTADOS_VALIDOS = ["pending", "shipped", "delivered", "cancelled"]


def leer(nombre):
    return (spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(f"datos/{nombre}.csv"))


def leer_inc(nombre, columnas):
    df = (spark.read.option("primitivesAsString", True)
          .json(f"datos/incremental/{nombre}.jsonl")
          .select(*columnas))
    return df.unionByName(df) if DUPLICAR_LOTE else df


def titulo(t):
    print(f"\n{'=' * 60}\n{t}\n{'=' * 60}")


customers_raw = leer("customers")
products_raw = leer("products")
orders_hist = leer("orders")
items_hist = leer("order_items")

# incremental: se toma el registro mas reciente por order_id (updated_at) y se descartan duplicados exactos
if INCREMENTAL:
    orders_inc = leer_inc("orders_incremental", orders_hist.columns)
    items_inc = leer_inc("order_items_incremental", items_hist.columns)

    orders_all = (orders_hist.withColumn("fuente", F.lit(0))
                  .unionByName(orders_inc.withColumn("fuente", F.lit(1))))
    w = Window.partitionBy("order_id").orderBy(
        F.try_to_timestamp("updated_at").desc_nulls_last(),
        F.col("fuente").desc(),
    )
    orders_raw = (orders_all.withColumn("rn", F.row_number().over(w))
                  .filter("rn = 1").drop("rn", "fuente"))
    items_raw = items_hist.unionByName(items_inc).dropDuplicates()
else:
    orders_raw = orders_hist
    items_raw = items_hist

assert items_raw.count() == items_raw.select("line_id").distinct().count(), \
    "line_id duplicado con contenido distinto"

# Verificar clientes y eliminar duplicados exactos
customers = customers_raw.dropDuplicates()
assert customers.count() == customers.select("customer_id").distinct().count(), \
    "customer_id sigue duplicado"

# Verificar precio como decimal; precio nulo o <= 0 es invalido
products = (
    products_raw
    .withColumn("price", F.col("unit_price").try_cast("decimal(10,2)"))
    .withColumn("motivo_prod",
                F.when(F.col("price").isNull() | (F.col("price") <= 0),
                       "precio_invalido"))
)

# Verificar pedidos y eliminar duplicados exactos, typo en estado, fecha, cliente
orders_d = orders_raw.dropDuplicates()
assert orders_d.count() == orders_d.select("order_id").distinct().count(), \
    "order_id sigue duplicado"

cli = customers.select(F.col("customer_id").alias("c_id"), "country")

orders = (
    orders_d
    .join(cli, orders_d.customer_id == cli.c_id, "left")
    .withColumn("status_fix",
                F.when(F.col("status") == "deliverd", "delivered")
                .otherwise(F.col("status")))
    .withColumn("ts", F.try_to_timestamp("order_ts"))
    .withColumn("motivo",
                F.when(F.col("ts").isNull(), "fecha_invalida")
                .when(F.col("customer_id").isNull(), "cliente_nulo")
                .when(F.col("c_id").isNull(), "cliente_inexistente")
                .when(~F.col("status_fix").isin(ESTADOS_VALIDOS), "estado_invalido"))
)

# Verificar tipos, rangos y referencias
o_flags = orders.select(
    F.col("order_id").alias("o_id"),
    F.col("motivo").alias("motivo_pedido"),
    F.col("customer_id").alias("o_customer_id"),
    F.col("ts").alias("order_ts_t"),
    F.col("status_fix").alias("status_t"),
    F.col("channel").alias("channel_t"),
    F.col("country").alias("country_t"),
)
p_flags = products.select(
    F.col("product_id").alias("p_id"),
    "motivo_prod",
    "price",
    F.col("category").alias("category_t"),
)

items = (
    items_raw
    .withColumn("qty", F.col("quantity").try_cast("int"))
    .withColumn("disc", F.col("discount_pct").try_cast("decimal(5,4)"))
    .join(o_flags, items_raw.order_id == o_flags.o_id, "left")
    .join(p_flags, items_raw.product_id == p_flags.p_id, "left")
    .withColumn(
        "motivo",
        F.when(F.col("qty").isNull() | (F.col("qty") <= 0), "cantidad_invalida")
        .when(F.col("disc").isNull() | (F.col("disc") < 0) | (F.col("disc") > 1),
              "descuento_fuera_de_rango")
        .when(F.col("o_id").isNull(), "pedido_inexistente")
        .when(F.col("motivo_pedido").isNotNull(), "pedido_en_cuarentena")
        .when(F.col("p_id").isNull(), "producto_inexistente")
        .when(F.col("motivo_prod").isNotNull(), "producto_precio_invalido"),
    )
)

# Verificar el detalle valido + el importe neto (cantidad * precio * (1 - descuento))
detalle = (
    items.filter(F.col("motivo").isNull())
    .select(
        "line_id", "order_id", "product_id",
        F.col("o_customer_id").alias("customer_id"),
        F.col("country_t").alias("country"),
        F.col("channel_t").alias("channel"),
        F.col("status_t").alias("status"),
        F.col("order_ts_t").alias("order_ts"),
        F.date_format("order_ts_t", "yyyy-MM").alias("year_month"),
        F.col("category_t").alias("category"),
        F.col("qty").alias("quantity"),
        F.col("price").alias("unit_price"),
        F.col("disc").alias("discount_pct"),
    )
    .withColumn(
        "net_amount",
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct")), 2)
        .cast("decimal(18,2)"),
    )
)

# Verificar que el net_amount sea positivo (precio * cantidad * (1 - descuento))
def a_cuarentena(df, tabla, id_col, cols):
    return df.select(
        F.lit(tabla).alias("tabla"),
        F.col(id_col).alias("id"),
        "motivo",
        F.to_json(F.struct(*[F.col(c) for c in cols])).alias("registro"),
    )


q_orders = a_cuarentena(orders.filter(F.col("motivo").isNotNull()),
                        "orders", "order_id", orders_raw.columns)
q_items = a_cuarentena(items.filter(F.col("motivo").isNotNull()),
                       "order_items", "line_id", items_raw.columns)
q_products = a_cuarentena(
    products.filter(F.col("motivo_prod").isNotNull())
    .withColumnRenamed("motivo_prod", "motivo"),
    "products", "product_id", products_raw.columns)

cuarentena = q_orders.unionByName(q_items).unionByName(q_products)

# guardar resultados
detalle.write.mode("overwrite").parquet("output/detalle_ventas")
cuarentena.write.mode("overwrite").parquet("output/cuarentena")

# contar registros y mostrar resumen
modo = "historico + incremental" if INCREMENTAL else "solo historico"
if INCREMENTAL and DUPLICAR_LOTE:
    modo += " (lote entregado DOS veces)"
titulo(f"RESUMEN DE LIMPIEZA - {modo}")
print("clientes: crudos", customers_raw.count(), "-> unicos", customers.count())
print("pedidos : historico", orders_hist.count(), "-> unicos tras limpiar/incremental",
      orders_d.count())

n_raw = items_raw.count()
n_ok = detalle.count()
n_q = items.filter(F.col("motivo").isNotNull()).count()
print(f"lineas  : entrada {n_raw} = validas {n_ok} + cuarentena {n_q}  ->",
      "OK" if n_raw == n_ok + n_q else "DESCUADRE")

if INCREMENTAL:
    titulo("PEDIDOS HISTORICOS ACTUALIZADOS POR EL LOTE")
    ids_inc = orders_inc.select("order_id").distinct()
    antes = (orders_hist.join(ids_inc, "order_id", "left_semi").dropDuplicates()
             .select("order_id",
                     F.col("status").alias("estado_antes"),
                     F.col("updated_at").alias("updated_antes")))
    ahora = orders_raw.select("order_id",
                              F.col("status").alias("estado_ahora"),
                              F.col("updated_at").alias("updated_ahora"))
    antes.join(ahora, "order_id").show(truncate=False)

titulo("CUARENTENA POR TABLA Y MOTIVO")
cuarentena.groupBy("tabla", "motivo").count().orderBy("tabla", "motivo").show(truncate=False)

titulo("HUELLA DEL RESULTADO (debe ser identica al reprocesar el lote)")
print("pedidos unicos:", orders_d.count(),
      "| lineas validas:", n_ok,
      "| suma neta:", detalle.agg(F.sum("net_amount")).first()[0])

spark.stop()
