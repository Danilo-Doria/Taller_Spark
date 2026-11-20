# Taller local de Data Engineering: ETL de ventas con PySpark

ETL local (sin cloud, sin Delta Lake) sobre datos sintéticos de una tienda latinoamericana.
Lee CSV y JSONL con Spark en modo local, perfila y limpia los datos, calcula el importe neto
por línea, agrega las ventas por mes, país, canal y categoría, y guarda el resultado en Parquet.
Incluye un lote incremental con salida idempotente.

## Requisitos

- Python 3.12
- Java 17, 21 o 25 (`java -version`)
- `pyspark==4.3.0`
- `openpyxl` (solo para convertir el Excel a CSV/JSONL, se usa una vez)

```bash
python3.12 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
python -m pip install pyspark==4.3.0 openpyxl
```

## Estructura

```
Taller_Spark/
├── datos/
│   ├── taller_data_engineer.xlsx        # fuente original (solo para explorar y convertir)
│   ├── customers.csv                    # generados por conversor.py
│   ├── products.csv
│   ├── orders.csv
│   ├── order_items.csv
│   └── incremental/
│       ├── orders_incremental.jsonl
│       └── order_items_incremental.jsonl
├── output/                              # salidas (Parquet y reportes)
│   ├── detalle_ventas/                  # una fila por línea de pedido válida
│   ├── cuarentena/                      # registros rechazados, con motivo
│   ├── ventas_mensuales/                # agregado por mes, país, canal, categoría
│   └── validador_resumen.txt            # reporte de perfilamiento
├── src/
│   ├── __init__.py                      # vacío; no se usa al ejecutar los scripts
│   ├── conversor.py                     # Excel -> CSV y JSONL
│   ├── validador_env.py                 # comprueba Spark y la lectura de los 6 archivos
│   ├── validador.py                     # perfilamiento y diagnóstico de calidad
│   ├── limpieza.py                      # limpieza, joins, importe neto, incremental
│   └── agregacion.py                    # ventas agregadas y controles de calidad
├── README.md
└── Taller Spark.pdf                     # enunciado del taller
```

## Cómo ejecutarlo

Siempre desde la raíz del proyecto (`Taller_Spark/`), con el entorno virtual activo.

| Orden | Script | Comando | Qué hace |
|---|---|---|---|
| 1 | `conversor.py` | `python src/conversor.py` | Convierte las hojas del Excel en `customers.csv`, `products.csv`, `orders.csv`, `order_items.csv` y los dos JSONL de `datos/incremental/`. Se ejecuta una sola vez. |
| 2 | `validador_env.py` | `python src/validador_env.py` | Comprueba que Spark arranca y lee los 6 archivos (imprime versión y conteos). |
| 3 | `validador.py` | `python src/validador.py` | Perfilamiento: nulos, duplicados, dominios, rangos y referencias huérfanas. Para guardar el reporte, usa el comando de abajo. |
| 4 | `limpieza.py` | `python src/limpieza.py` | Limpia, une, calcula el importe neto, aplica el lote incremental y escribe `output/detalle_ventas` y `output/cuarentena`. |
| 5 | `agregacion.py` | `python src/agregacion.py` | Agrega las ventas, escribe `output/ventas_mensuales` y ejecuta los dos controles de calidad. |

Para guardar el reporte de perfilamiento en `output/validador_resumen.txt` (mostrándolo también en pantalla):

```bash
python src/validador.py 2>&1 | grep -v -E "WARN|^\s+at |IllegalArgument|incubator|log4j|setLogLevel" | tee output/validador_resumen.txt
```

Versión corta, una vez que `datos/` ya tiene los CSV y JSONL:

```bash
python src/limpieza.py
python src/agregacion.py
```

El orden importa: `agregacion.py` lee `output/detalle_ventas`, que produce `limpieza.py`.
Ambos se pueden repetir sin duplicar resultados.

Para ver menos ruido de Spark en la terminal, añade al final de cualquier comando:

```bash
2>&1 | grep -v -E "WARN|^\s+at |IllegalArgument|incubator|log4j|setLogLevel"
```

Opciones de `limpieza.py`:

| Opción | Efecto |
|---|---|
| (ninguna) | Histórico + lote incremental |
| `--sin-incremental` | Solo el histórico |
| `--duplicar-lote` | Simula recibir el lote dos veces (prueba de idempotencia) |

## Modelo de datos

| Archivo | Clave | Granularidad |
|---|---|---|
| customers.csv | customer_id | un cliente |
| products.csv | product_id | un producto |
| orders.csv | order_id | un pedido |
| order_items.csv | line_id | una línea de pedido |

Relaciones: `orders.customer_id` -> `customers`, `order_items.order_id` -> `orders`,
`order_items.product_id` -> `products`.

Importe neto de cada línea: `quantity * unit_price * (1 - discount_pct)`, con decimales
(`decimal(18,2)`), no con `double`.

## Anomalías detectadas y tratamiento

| Tratamiento | Caso |
|---|---|
| Corregir | `customer_id` duplicado exacto (C0020) y `order_id` duplicado exacto (O00012): se deduplican |
| Corregir | Estado `deliverd` -> `delivered` |
| Cuarentena | Fecha imposible en `order_ts` (O00025, mes 99) |
| Cuarentena | Pedido sin cliente (O00011) o con cliente inexistente (O00007, C9999) |
| Cuarentena | Línea con pedido inexistente (O99999) o producto inexistente (P999) |
| Cuarentena | Cantidad <= 0 (2 líneas) y descuento fuera de [0, 1] (1 línea, 1,2) |
| Cuarentena | Precio inválido: P088 (-12,5) y P089 (nulo); sus líneas no se pueden valorar |
| Mantener | Email nulo en un cliente: no afecta las ventas |

Criterio: **corregir** cuando la respuesta es inequívoca; **cuarentena** cuando el dato podría
recuperarse pero no es confiable; **rechazar** cuando no tiene valor de negocio. Nada se descarta
en silencio: todo lo que sale del flujo principal queda en `output/cuarentena` con la tabla, el id,
el motivo y el registro original en JSON.

Impacto a tener en cuenta: las líneas de P088 y P089 (827 con el lote incluido) quedan fuera de
las ventas, porque no se inventa un precio.

## Incremental idempotente

1. Se unen el histórico y el lote.
2. De cada `order_id` se conserva la versión con `updated_at` más reciente
   (ventana + `row_number`; en caso de empate gana la fila del lote).
3. Las líneas se deduplican por `line_id`.
4. La salida se reconstruye completa con `mode("overwrite")`.

Reprocesar el mismo lote, o recibirlo dos veces, da el mismo resultado. Prueba (huella):

| Corrida | Pedidos únicos | Líneas válidas | Suma neta |
|---|---|---|---|
| Solo histórico | 20.000 | 39.190 | 95.297.657,67 |
| Histórico + lote | 20.100 | 39.386 | 95.703.956,43 |
| Lote reprocesado | 20.100 | 39.386 | 95.703.956,43 |
| Lote entregado dos veces | 20.100 | 39.386 | 95.703.956,43 |

El pedido histórico O00005 recibe una actualización tardía (`updated_at` pasa de 2026-02-22 a
2026-03-05). En estos datos solo cambia esa fecha, no el estado.

Limitación: reconstruir todo es correcto y simple con este volumen. Con volúmenes grandes se
usaría un `MERGE` (por ejemplo con Delta Lake) o se reescribirían solo las particiones afectadas.

## Controles de calidad

| Control | Qué verifica | Resultado |
|---|---|---|
| Conservación de líneas | 40.226 de entrada = 39.386 válidas + 840 en cuarentena | OK |
| Reconciliación de importes | Total agregado = total del detalle (95.703.956,43); también sin cancelados (72.087.222,04) | OK |
| Unicidad | 39.386 líneas en el detalle = 39.386 distintas = 39.386 sumadas en el agregado | OK |

Los joins se hacen contra claves únicas y la agregación parte de un detalle con una fila por línea,
así que no se multiplican los importes.

## Notas de lectura de resultados

- `ventas_mensuales` entrega dos columnas: `ventas_netas_total` y `ventas_netas_sin_cancelados`.
  Alrededor del 25 % de los pedidos está cancelado; como cifra principal conviene la segunda,
  porque un pedido cancelado no genera ingreso.
- Enero y febrero tienen historia completa. Marzo es parcial, y abril a junio salen solo del
  lote incremental: no son comparables con enero y febrero ni indican una caída de ventas.

## Decisiones técnicas

- **Zona horaria UTC** (`spark.sql.session.timeZone`): las fechas vienen en UTC (sufijo `Z`) y, sin
  fijarla, el mes de un pedido cambiaría según la zona de la máquina.
- **Modo ANSI de Spark 4:** un valor mal formado lanza un error en vez de dar `NULL`. Por eso se usan
  `try_cast` y `try_to_timestamp`, y los valores inválidos se envían a cuarentena.
- **Spark UI desactivada** (`spark.ui.enabled=false`): no se necesita y evita avisos de Java cuando
  la ruta del proyecto contiene caracteres con tilde.
- **Todo se lee como texto** (`inferSchema=False`) y los tipos se convierten de forma explícita.

## Límites del modo local

Corre en una sola JVM: no hay red entre nodos, tolerancia a fallos ni particionado real, y el tiempo
de ejecución depende de la máquina, no es un benchmark de clúster.

---

## 👨💻 Author

- GitHub: **[Danilo-Doria](https://github.com/Danilo-Doria)**
- LinkedIn: **[Danilo Doria Diaz](https://www.linkedin.com/in/danilo-doria-diaz)**
- Mail: **danilodoria519@gmail.com**
