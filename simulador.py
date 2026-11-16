#!/usr/bin/env python3
"""Simulador de máquinas (corre en EC2, no en Lambda).

Cada ciclo (60 s por defecto) genera las lecturas de TODAS las máquinas y publica
UN solo mensaje (el lote del minuto) en el tema SNS TemaLecturas.
La ingesta (S3 + DynamoDB + Step Functions) la hace la Lambda IngestarLote.

Variables de entorno:
  SNS_TOPIC_ARN        (obligatoria) ARN de TemaLecturas-<stage>
  TABLA_MAQUINAS       (opcional)    t_maquinas-dev; se usa solo para retomar el estado al reiniciar
  NUM_MAQUINAS         (opcional)    30
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

REGION = os.environ.get("AWS_REGION", "us-east-1")
TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
TABLA_MAQUINAS = os.environ.get("TABLA_MAQUINAS", "t_maquinas-dev")
NUM_MAQUINAS = int(os.environ.get("NUM_MAQUINAS", "30"))
INTERVALO = int(os.environ.get("INTERVALO_SEGUNDOS", "60"))

sns = boto3.client("sns", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)

# Las 3 máquinas de demostración originales, más las que falten hasta NUM_MAQUINAS.
# Sustituyan coordenadas y valores base por los del caso real.
BASE = [
    {"machine_id": "MAQ-001", "lat": -12.0464, "lon": -77.0428, "temperatura": 75.0, "vibracion": 3.0,
     "presion": 150.0, "rpm": 1600, "combustible": 12.0, "carga": 8.0, "velocidad": 15.0},
    {"machine_id": "MAQ-002", "lat": -12.0550, "lon": -77.0300, "temperatura": 80.0, "vibracion": 4.0,
     "presion": 160.0, "rpm": 1800, "combustible": 15.0, "carga": 10.0, "velocidad": 18.0},
    {"machine_id": "MAQ-003", "lat": -12.0350, "lon": -77.0500, "temperatura": 70.0, "vibracion": 2.5,
     "presion": 145.0, "rpm": 1500, "combustible": 10.0, "carga": 6.0, "velocidad": 12.0},
]
CAMPOS_NUMERICOS = ["temperatura", "vibracion", "presion", "rpm", "combustible", "carga", "velocidad"]
AMPLITUD = {"temperatura": 1.5, "vibracion": 0.3, "presion": 2.0, "rpm": 50,
            "combustible": 0.5, "carga": 0.5, "velocidad": 1.0}


def construir_flota():
    rng = random.Random(42)  # semilla fija: la flota es la misma en cada arranque
    flota = [dict(m) for m in BASE[:NUM_MAQUINAS]]
    for i in range(len(flota) + 1, NUM_MAQUINAS + 1):
        flota.append({
            "machine_id": f"MAQ-{i:03d}",
            "lat": round(-12.0464 + rng.uniform(-0.06, 0.06), 4),
            "lon": round(-77.0428 + rng.uniform(-0.06, 0.06), 4),
            "temperatura": round(rng.uniform(68, 82), 1),
            "vibracion": round(rng.uniform(2.2, 4.2), 2),
            "presion": round(rng.uniform(140, 165), 1),
            "rpm": rng.randint(1450, 1850),
            "combustible": round(rng.uniform(9, 16), 1),
            "carga": round(rng.uniform(5, 11), 1),
            "velocidad": round(rng.uniform(11, 19), 1),
        })
    return flota


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
