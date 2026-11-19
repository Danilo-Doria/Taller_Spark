from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("agregacion")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def titulo(t):
    print(f"\n{'=' * 60}\n{t}\n{'=' * 60}")

detalle = spark.read.parquet("output/detalle_ventas")
detalle.createOrReplaceTempView("detalle")

ventas = spark.sql("""
    SELECT
        year_month,
        country,
        channel,
        category,
        COUNT(DISTINCT order_id)                                   AS pedidos,
        COUNT(*)                                                   AS lineas,
        CAST(SUM(net_amount) AS DECIMAL(18,2))                     AS ventas_netas_total,
        CAST(SUM(CASE WHEN status <> 'cancelled' THEN net_amount ELSE 0 END)
             AS DECIMAL(18,2))                                     AS ventas_netas_sin_cancelados
    FROM detalle
    GROUP BY year_month, country, channel, category
    ORDER BY year_month, country, channel, category
""")

ventas.write.mode("overwrite").parquet("output/ventas_mensuales")

titulo("MUESTRA: ventas por mes, pais, canal y categoria")
ventas.show(10, truncate=False)

titulo("VENTAS POR MES")
(ventas.groupBy("year_month")
 .agg(F.sum("ventas_netas_total").alias("total"),
      F.sum("ventas_netas_sin_cancelados").alias("sin_cancelados"))
 .orderBy("year_month").show())

# Total agregado se reconcilia con el total del detalle
titulo("CONTROL 1: reconciliacion de importes")
total_detalle = detalle.agg(F.sum("net_amount")).first()[0]
total_agregado = ventas.agg(F.sum("ventas_netas_total")).first()[0]
sc_detalle = detalle.filter("status <> 'cancelled'").agg(F.sum("net_amount")).first()[0]
sc_agregado = ventas.agg(F.sum("ventas_netas_sin_cancelados")).first()[0]
print("total detalle   :", total_detalle)
print("total agregado  :", total_agregado)
print("sin cancelados detalle  :", sc_detalle)
print("sin cancelados agregado :", sc_agregado)
print("->", "OK" if (total_detalle == total_agregado and sc_detalle == sc_agregado)
      else "DESCUADRE")

# Revision de linea quedo duplicada o perdida en el agregado
titulo("CONTROL 2: unicidad de lineas y conteo")
n_lineas = detalle.count()
n_distintas = detalle.select("line_id").distinct().count()
n_agregadas = ventas.agg(F.sum("lineas")).first()[0]
print("lineas en detalle:", n_lineas, "| distintas:", n_distintas,
      "| sumadas en el agregado:", n_agregadas)
print("->", "OK" if (n_lineas == n_distintas == n_agregadas) else "DESCUADRE")

spark.stop()
