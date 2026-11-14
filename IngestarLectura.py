import json
import os
import re
from decimal import Decimal
from datetime import datetime
from urllib.parse import quote

import boto3

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
stepfunctions = boto3.client("stepfunctions")

BUCKET = os.environ["S3_BUCKET"]
TABLA_MAQUINAS = dynamodb.Table(os.environ["TABLA_MAQUINAS"])
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]

CAMPOS_REQUERIDOS = {
    "machine_id", "timestamp", "lat", "lon", "temperatura", "vibracion",
    "presion", "rpm", "combustible", "carga", "velocidad", "horas_uso", "estado"
}

def validar_lectura(lectura):
    faltantes = CAMPOS_REQUERIDOS - set(lectura.keys())
    if faltantes:
        raise ValueError(f"Faltan campos: {sorted(faltantes)}")
    if not isinstance(lectura["machine_id"], str) or not lectura["machine_id"]:
        raise ValueError("machine_id debe ser un texto no vacío")
    datetime.fromisoformat(lectura["timestamp"].replace("Z", "+00:00"))
    for campo in CAMPOS_REQUERIDOS - {"machine_id", "timestamp", "estado"}:
        float(lectura[campo])

def lambda_handler(event, context):
    fallidos = []

    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            lectura = json.loads(record["body"])
            validar_lectura(lectura)

            machine_id = lectura["machine_id"]
            timestamp = lectura["timestamp"]
            fecha = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).date().isoformat()

            # Historial inmutable en S3, organizado por máquina y fecha.
            clave = (
                f"lecturas/machine_id={quote(machine_id, safe='')}/"
                f"fecha={fecha}/{quote(timestamp, safe='')}.json"
            )
            s3.put_object(
                Bucket=BUCKET,
                Key=clave,
                Body=json.dumps(lectura).encode("utf-8"),
                ContentType="application/json"
            )

            # Estado más reciente de la máquina en DynamoDB.
            item_dynamo = {
                k: Decimal(str(v)) if isinstance(v, (int, float)) and not isinstance(v, bool) else v
                for k, v in lectura.items()
            }
            TABLA_MAQUINAS.put_item(Item=item_dynamo)

            # Una ejecución de análisis por lectura recibida.
            nombre = re.sub(r"[^A-Za-z0-9_-]", "-", f"{machine_id}-{timestamp}")[:80]
            try:
                stepfunctions.start_execution(
                    stateMachineArn=STATE_MACHINE_ARN,
                    name=nombre,
                    input=json.dumps({
                        "machine_id": machine_id,
                        "timestamp": timestamp,
                        "lat": lectura["lat"],
                        "lon": lectura["lon"]
                    })
                )
            except stepfunctions.exceptions.ExecutionAlreadyExists:
                # El mensaje pudo haberse reintentado después de iniciar el análisis.
                print(f"El análisis de {machine_id} para {timestamp} ya fue iniciado.")

        except Exception as exc:
            print(f"Error procesando mensaje {message_id}: {exc}")
            fallidos.append({"itemIdentifier": message_id})

    return {"batchItemFailures": fallidos}
