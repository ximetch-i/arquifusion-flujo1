import json
import os
import re
from datetime import datetime
from decimal import Decimal
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


def guardar_en_s3(lectura):
    machine_id = lectura["machine_id"]
    timestamp = lectura["timestamp"]
    fecha = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).date().isoformat()
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


def procesar_lote(lote):
    """Valida, guarda y lanza UNA ejecución de Step Functions para todo el lote."""
    lecturas = lote["lecturas"] if isinstance(lote, dict) else lote
    validas = []
    for lectura in lecturas:
        try:
            validar_lectura(lectura)
            validas.append(lectura)
        except Exception as exc:
            # Una lectura inválida no debe reintentar (ni mandar a la DLQ) todo el lote.
            print(f"Lectura descartada ({lectura.get('machine_id')}): {exc}")

    if not validas:
        raise ValueError("El lote no tiene lecturas válidas")

    for lectura in validas:
        guardar_en_s3(lectura)

    # Estado más reciente de cada máquina (escritura en lote).
    with TABLA_MAQUINAS.batch_writer(overwrite_by_pkeys=["machine_id"]) as escritor:
        for lectura in validas:
            escritor.put_item(Item={
                k: Decimal(str(v)) if isinstance(v, (int, float)) and not isinstance(v, bool) else v
                for k, v in lectura.items()
            })

    lote_id = (lote.get("lote_id") if isinstance(lote, dict) else None) or validas[0]["timestamp"]
    nombre = re.sub(r"[^A-Za-z0-9_-]", "-", f"lote-{lote_id}")[:80]
    entrada = {
        "lote_id": lote_id,
        "maquinas": [
            {"machine_id": x["machine_id"], "timestamp": x["timestamp"], "lat": x["lat"], "lon": x["lon"]}
            for x in validas
        ],
    }
    try:
        stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=nombre,
            input=json.dumps(entrada)
        )
    except stepfunctions.exceptions.ExecutionAlreadyExists:
        # El mensaje pudo reintentarse después de iniciar el análisis.
        print(f"El análisis del lote {lote_id} ya fue iniciado.")
    return len(validas)


def lambda_handler(event, context):
    fallidos = []
    for record in event.get("Records", []):
        try:
            n = procesar_lote(json.loads(record["body"]))
            print(f"Lote procesado: {n} lecturas")
        except Exception as exc:
            print(f"Error procesando mensaje {record['messageId']}: {exc}")
            fallidos.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": fallidos}
