import csv
import sys
import time
import json
from kafka import KafkaProducer

RUTA = sys.argv[1] if len(sys.argv) > 1 else "/home/vboxuser/covid_colombia.csv"


def a_entero(valor):
    try:
        return int(float(valor))
    except (TypeError, ValueError):
        return None


producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                         value_serializer=lambda x: json.dumps(x).encode("utf-8"))

print(f"Leyendo casos desde {RUTA} y enviándolos al topic 'casos_covid'...")
while True:  # al terminar el archivo vuelve a empezar, para que el flujo no se detenga
    with open(RUTA, encoding="utf-8") as f:
        for fila in csv.DictReader(f):
            caso = {
                "id_caso": a_entero(fila.get("id_de_caso")),
                "departamento": (fila.get("departamento_nom") or "").strip().upper(),
                "edad": a_entero(fila.get("edad")),
                "sexo": (fila.get("sexo") or "").strip().upper(),
                "estado": (fila.get("estado") or "").strip().upper(),
                # Hora de llegada del evento (momento en que se envía)
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            if not caso["departamento"]:
                continue
            producer.send("casos_covid", value=caso)
            print(f"Enviado: {caso}")
            time.sleep(0.5)