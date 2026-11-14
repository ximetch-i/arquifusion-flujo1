import json
import os
import random
from datetime import datetime, timezone
from decimal import Decimal

import boto3

sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")

TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
TABLA_MAQUINAS = dynamodb.Table(os.environ["TABLA_MAQUINAS"])

# Datos de demostración: reemplazar coordenadas y máquinas por las del escenario.
MAQUINAS = [
    {
        "machine_id": "MAQ-001", "lat": -12.0464, "lon": -77.0428,
        "temperatura": 75.0, "vibracion": 3.0, "presion": 150.0,
        "rpm": 1600, "combustible": 12.0, "carga": 8.0, "velocidad": 15.0
    },
    {
        "machine_id": "MAQ-002", "lat": -12.0550, "lon": -77.0300,
        "temperatura": 80.0, "vibracion": 4.0, "presion": 160.0,
        "rpm": 1800, "combustible": 15.0, "carga": 10.0, "velocidad": 18.0
    },
    {
        "machine_id": "MAQ-003", "lat": -12.0350, "lon": -77.0500,
        "temperatura": 70.0, "vibracion": 2.5, "presion": 145.0,
        "rpm": 1500, "combustible": 10.0, "carga": 6.0, "velocidad": 12.0
    }
]

def variacion(valor, amplitud):
    return max(0, float(valor) + random.uniform(-amplitud, amplitud))

def generar_lectura(config):
    """Mantiene continuidad leyendo el último estado guardado en t_maquinas."""
    respuesta = TABLA_MAQUINAS.get_item(Key={"machine_id": config["machine_id"]})
    anterior = respuesta.get("Item", {})

    lectura = {
        "machine_id": config["machine_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "lat": config["lat"],
        "lon": config["lon"],
        "temperatura": round(variacion(anterior.get("temperatura", config["temperatura"]), 1.5), 2),
        "vibracion": round(variacion(anterior.get("vibracion", config["vibracion"]), 0.3), 2),
        "presion": round(variacion(anterior.get("presion", config["presion"]), 2.0), 2),
        "rpm": round(variacion(anterior.get("rpm", config["rpm"]), 50), 2),
        "combustible": round(variacion(anterior.get("combustible", config["combustible"]), 0.5), 2),
        "carga": round(variacion(anterior.get("carga", config["carga"]), 0.5), 2),
        "velocidad": round(variacion(anterior.get("velocidad", config["velocidad"]), 1.0), 2),
        "horas_uso": round(float(anterior.get("horas_uso", 1000)) + 1 / 60, 4),
        "estado": "operativa"
    }

    # Anomalía artificial de baja frecuencia para probar el recorrido.
    # No representa una probabilidad real de falla.
    if random.random() < 0.03:
        lectura["temperatura"] = round(lectura["temperatura"] + random.uniform(10, 20), 2)
        lectura["vibracion"] = round(lectura["vibracion"] + random.uniform(2, 4), 2)

    return lectura

def lambda_handler(event, context):
    publicadas = 0

    for config in MAQUINAS:
        lectura = generar_lectura(config)

        # DynamoDB no acepta float nativo: convertir valores numéricos a Decimal.
        item = {
            k: Decimal(str(v)) if isinstance(v, (int, float)) else v
            for k, v in lectura.items()
        }
        TABLA_MAQUINAS.put_item(Item=item)

        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject=f"Lectura {lectura['machine_id']}",
            Message=json.dumps(lectura)
        )
        publicadas += 1

    return {"statusCode": 200, "publicadas": publicadas}
