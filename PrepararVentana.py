import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import urllib.request
import urllib.error

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

BUCKET = os.environ["S3_BUCKET"]
GCP_API_URL = os.environ.get("GCP_API_URL", "").strip()
GCP_API_KEY_PARAMETER = os.environ["GCP_API_KEY_PARAMETER"]
AI_MODE = os.environ.get("AI_MODE", "MOCK").upper()
VENTANA_MINUTOS = int(os.environ.get("VENTANA_MINUTOS", "10"))

def leer_lecturas_recientes(machine_id, instante):
    """Lee de S3 las lecturas de la máquina dentro de la ventana temporal."""
    inicio = instante - timedelta(minutes=VENTANA_MINUTOS)
    lecturas = []

    # Se revisan la fecha actual y la anterior para cubrir ventanas que cruzan medianoche.
    for fecha in {instante.date(), inicio.date()}:
        prefijo = f"lecturas/machine_id={machine_id}/fecha={fecha.isoformat()}/"
        paginator = s3.get_paginator("list_objects_v2")

        for pagina in paginator.paginate(Bucket=BUCKET, Prefix=prefijo):
            for objeto in pagina.get("Contents", []):
                contenido = s3.get_object(Bucket=BUCKET, Key=objeto["Key"])["Body"].read()
                lectura = json.loads(contenido)
                ts = datetime.fromisoformat(lectura["timestamp"].replace("Z", "+00:00"))
                if inicio <= ts <= instante:
                    lecturas.append(lectura)

    lecturas.sort(key=lambda x: x["timestamp"])
    return lecturas

def llamar_modelo(machine_id, lecturas):
    if AI_MODE == "MOCK":
        # Respuesta de prueba para desarrollar el flujo sin depender aún de Google Cloud.
        max_temp = max(float(x["temperatura"]) for x in lecturas)
        max_vib = max(float(x["vibracion"]) for x in lecturas)
        score = min(1.0, max(0.0, (max_temp - 70) / 50, (max_vib - 2) / 10))
        return {
            "puntaje_anomalia": round(score, 4),
            "prob_falla": round(score, 4),
            "variables_relevantes": ["temperatura", "vibracion"]
        }

    if not GCP_API_URL:
        raise RuntimeError("Falta configurar GCP_API_URL")

    parametro = ssm.get_parameter(
        Name=GCP_API_KEY_PARAMETER,
        WithDecryption=True
    )
    api_key = parametro["Parameter"]["Value"]

    cuerpo = json.dumps({
        "machine_id": machine_id,
        "ventana": lecturas
    }).encode("utf-8")
    solicitud = urllib.request.Request(
        GCP_API_URL.rstrip("/") + "/analizar",
        data=cuerpo,
        headers={
            "x-api-key": api_key,
            "Content-Type": "application/json"
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(solicitud, timeout=45) as respuesta:
            return json.loads(respuesta.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detalle = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Cloud Run respondió HTTP {exc.code}: {detalle}") from exc

def lambda_handler(event, context):
    machine_id = event["machine_id"]
    instante = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    lecturas = leer_lecturas_recientes(machine_id, instante)

    if not lecturas:
        raise ValueError(f"No hay lecturas en S3 para {machine_id}")

    resultado = llamar_modelo(machine_id, lecturas)

    return {
        "machine_id": machine_id,
        "timestamp": event["timestamp"],
        "lat": event.get("lat", lecturas[-1]["lat"]),
        "lon": event.get("lon", lecturas[-1]["lon"]),
        "cantidad_lecturas_ventana": len(lecturas),
        "puntaje_anomalia": float(resultado["puntaje_anomalia"]),
        "prob_falla": float(resultado["prob_falla"]),
        "variables_relevantes": resultado.get("variables_relevantes", [])
    }
