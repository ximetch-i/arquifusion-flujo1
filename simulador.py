#!/usr/bin/env python3
"""Simulador de máquinas (corre en EC2, no en Lambda).

Cada ciclo (60 s por defecto) genera las lecturas de TODAS las máquinas y publica
UN solo mensaje (el lote del minuto) en el tema SNS TemaLecturas.
La ingesta (S3 + DynamoDB + Step Functions) la hace la Lambda IngestarLote.

Variables de entorno:
  SNS_TOPIC_ARN        (obligatoria) ARN de TemaLecturas-<stage>
  TABLA_MAQUINAS       (opcional)    t_maquinas-dev; se usa solo para retomar el estado al reiniciar
  NUM_MAQUINAS         (opcional)    30 (lo lee flota.py)
  INTERVALO_SEGUNDOS   (opcional)    60
  AWS_REGION           (opcional)    us-east-1
"""
import json
import os
import random
import signal
import time
from datetime import datetime, timezone

import boto3

from flota import construir_flota

REGION = os.environ.get("AWS_REGION", "us-east-1")
TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
TABLA_MAQUINAS = os.environ.get("TABLA_MAQUINAS", "t_maquinas-dev")
INTERVALO = int(os.environ.get("INTERVALO_SEGUNDOS", "60"))

sns = boto3.client("sns", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)

CAMPOS_NUMERICOS = ["temperatura", "vibracion", "presion", "rpm", "combustible", "carga", "velocidad"]
AMPLITUD = {"temperatura": 1.5, "vibracion": 0.3, "presion": 2.0, "rpm": 50,
            "combustible": 0.5, "carga": 0.5, "velocidad": 1.0}


def cargar_estado_previo(flota):
    """Retoma el último estado guardado en t_maquinas (solo al arrancar)."""
    estado = {m["machine_id"]: {c: float(m[c]) for c in CAMPOS_NUMERICOS} | {"horas_uso": 1000.0}
              for m in flota}
    try:
        ids = [m["machine_id"] for m in flota]
        for i in range(0, len(ids), 100):
            resp = dynamodb.batch_get_item(RequestItems={
                TABLA_MAQUINAS: {"Keys": [{"machine_id": x} for x in ids[i:i + 100]]}
            })
            for item in resp["Responses"].get(TABLA_MAQUINAS, []):
                e = estado[item["machine_id"]]
                for c in CAMPOS_NUMERICOS + ["horas_uso"]:
                    if c in item:
                        e[c] = float(item[c])
    except Exception as exc:  # sin tabla o sin permisos: se arranca con los valores base
        print(f"No se pudo leer el estado previo ({exc}); se usan valores base.", flush=True)
    return estado


def variacion(valor, amplitud):
    return max(0.0, valor + random.uniform(-amplitud, amplitud))


def generar_lote(flota, estado, ahora):
    lecturas = []
    for m in flota:
        e = estado[m["machine_id"]]
        for c in CAMPOS_NUMERICOS:
            e[c] = variacion(e[c], AMPLITUD[c])  # el estado base NO incluye anomalías
        e["horas_uso"] += INTERVALO / 3600

        lectura = {
            "machine_id": m["machine_id"],
            "timestamp": ahora,
            "lat": m["lat"],
            "lon": m["lon"],
            **{c: round(e[c], 2) for c in CAMPOS_NUMERICOS},
            "horas_uso": round(e["horas_uso"], 4),
            "estado": "operativa",
        }
        # Anomalía artificial de baja frecuencia (pico transitorio, no deriva permanente).
        # No representa una probabilidad real de falla.
        if random.random() < 0.03:
            lectura["temperatura"] = round(lectura["temperatura"] + random.uniform(10, 20), 2)
            lectura["vibracion"] = round(lectura["vibracion"] + random.uniform(2, 4), 2)
        lecturas.append(lectura)
    return {"lote_id": ahora, "timestamp": ahora, "lecturas": lecturas}


def main():
    detener = {"si": False}
    signal.signal(signal.SIGTERM, lambda *_: detener.update(si=True))
    signal.signal(signal.SIGINT, lambda *_: detener.update(si=True))

    flota = construir_flota()
    estado = cargar_estado_previo(flota)
    print(f"Simulador iniciado: {len(flota)} máquinas, 1 ciclo cada {INTERVALO} s", flush=True)

    proximo = time.monotonic()
    while not detener["si"]:
        ahora = datetime.now(timezone.utc).isoformat(timespec="seconds")
        try:
            lote = generar_lote(flota, estado, ahora)
            sns.publish(TopicArn=TOPIC_ARN, Message=json.dumps(lote))
            print(f"{ahora} publicado lote de {len(lote['lecturas'])} lecturas", flush=True)
        except Exception as exc:  # un fallo de red no debe matar el servicio
            print(f"{ahora} ERROR publicando lote: {exc}", flush=True)

        proximo += INTERVALO
        while not detener["si"] and time.monotonic() < proximo:
            time.sleep(min(1.0, proximo - time.monotonic()))
    print("Simulador detenido.", flush=True)


if __name__ == "__main__":
    main()
