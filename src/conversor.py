import csv
import json
from pathlib import Path
from openpyxl import load_workbook

EXCEL = Path("datos/taller_data_engineer.xlsx")
SALIDA = Path("datos")
INCR = SALIDA / "incremental"
INCR.mkdir(parents=True, exist_ok=True)


def a_texto(v):
    if v is None:
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def filas(ws):
    it = ws.iter_rows(values_only=True)
    cols = list(next(it))
    for r in it:
        yield dict(zip(cols, (a_texto(v) for v in r)))


wb = load_workbook(EXCEL, read_only=True, data_only=True)

csvs = {
    "clientes": "customers.csv",
    "productos": "products.csv",
    "pedidos": "orders.csv",
    "detalle_pedidos": "order_items.csv",
}
for hoja, archivo in csvs.items():
    rows = list(filas(wb[hoja]))
    with open(SALIDA / archivo, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if v is None else v) for k, v in r.items()})
    print(archivo, len(rows))

jsonl = {
    "pedidos_incrementales": "orders_incremental.jsonl",
    "detalle_incremental": "order_items_incremental.jsonl",
}
for hoja, archivo in jsonl.items():
    rows = list(filas(wb[hoja]))
    with open(INCR / archivo, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print("incremental/" + archivo, len(rows))