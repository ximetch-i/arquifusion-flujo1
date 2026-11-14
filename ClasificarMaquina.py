import math
import os
from decimal import Decimal

import boto3

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

TABLA_RESULTADOS = dynamodb.Table(os.environ["TABLA_RESULTADOS"])
TEMA_INCIDENTES_ARN = os.environ["TEMA_INCIDENTES_ARN"]

UMBRAL_PREVENTIVO = float(os.environ.get("UMBRAL_PREVENTIVO", "0.50"))
UMBRAL_URGENTE = float(os.environ.get("UMBRAL_URGENTE", "0.80"))
RADIO_CAUSA_COMUN_KM = float(os.environ.get("RADIO_CAUSA_COMUN_KM", "5"))

def distancia_km(lat1, lon1, lat2, lon2):
    radio_tierra = 6371
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radio_tierra * math.asin(math.sqrt(a))

def buscar_maquinas_cercanas(lat, lon, machine_id):
    """Busca resultados recientes cercanos para señalar posible causa compartida."""
    respuesta = TABLA_RESULTADOS.scan()
    cercanas = []

    for item in respuesta.get("Items", []):
        if item.get("machine_id") == machine_id:
            continue
        if "lat" not in item or "lon" not in item:
            continue
        distancia = distancia_km(
            lat, lon, float(item["lat"]), float(item["lon"])
        )
        if distancia <= RADIO_CAUSA_COMUN_KM and item.get("severidad") in ("PREVENTIVO", "URGENTE"):
            cercanas.append(item["machine_id"])

    return sorted(set(cercanas))

def lambda_handler(event, context):
    prob = float(event["prob_falla"])
    puntaje = float(event["puntaje_anomalia"])

    if prob >= UMBRAL_URGENTE:
        severidad = "URGENTE"
    elif prob >= UMBRAL_PREVENTIVO or puntaje >= UMBRAL_PREVENTIVO:
        severidad = "PREVENTIVO"
    else:
        severidad = "NORMAL"

    machine_id = event["machine_id"]
    timestamp = event["timestamp"]
    lat = float(event["lat"])
    lon = float(event["lon"])

    cercanas = buscar_maquinas_cercanas(lat, lon, machine_id)
    resultado = {
        "machine_id": machine_id,
        "timestamp": timestamp,
        "lat": Decimal(str(lat)),
        "lon": Decimal(str(lon)),
        "puntaje_anomalia": Decimal(str(puntaje)),
        "prob_falla": Decimal(str(prob)),
        "variables_relevantes": event.get("variables_relevantes", []),
        "severidad": severidad,
        "maquinas_cercanas_con_alerta": cercanas,
        "posible_causa_comun": len(cercanas) > 0
    }

    TABLA_RESULTADOS.put_item(Item=resultado)

    if severidad == "URGENTE":
        sns.publish(
            TopicArn=TEMA_INCIDENTES_ARN,
            Subject=f"Incidente urgente: {machine_id}",
            Message=(
                f"machine_id={machine_id}; severidad=URGENTE; "
                f"prob_falla={prob}; lat={lat}; lon={lon}; "
                f"maquinas_cercanas={cercanas}"
            )
        )

    return {
        "machine_id": machine_id,
        "severidad": severidad,
        "prob_falla": prob,
        "puntaje_anomalia": puntaje,
        "posible_causa_comun": len(cercanas) > 0,
        "maquinas_cercanas_con_alerta": cercanas
    }
