#!/usr/bin/env python3
"""Carga/actualiza la flota en t_maquinas (zona, repuesto requerido, correo del operador).

Usa update_item: es seguro correrlo con el simulador activo y NO borra lo que ya exista.

  TABLA_MAQUINAS=t_maquinas-dev python3 seed_flota.py
"""
import os

import boto3

from flota import ZONAS, construir_flota

REGION = os.environ.get("AWS_REGION", "us-east-1")
TABLA = os.environ.get("TABLA_MAQUINAS", "t_maquinas-dev")
OPERADOR_EMAIL = os.environ.get("OPERADOR_EMAIL", "operador@example.com")
# Alterna los repuestos para probar los dos caminos del Flujo 2 (con y sin stock). Ajustar a los reales.
REPUESTOS = ["REP-001", "REP-002"]

tabla = boto3.resource("dynamodb", region_name=REGION).Table(TABLA)

for i, m in enumerate(construir_flota()):
    tabla.update_item(
        Key={"machine_id": m["machine_id"]},
        UpdateExpression=("SET zona = :z, repuesto_requerido = :r, operador_email = :o, "
                          "estado = if_not_exists(estado, :e)"),
        ExpressionAttributeValues={
            ":z": m["zona"],
            ":r": REPUESTOS[i % len(REPUESTOS)],
            ":o": OPERADOR_EMAIL,
            ":e": "OPERATIVA",
        },
    )

print(f"{TABLA}: {len(construir_flota())} máquinas cargadas")
print("Zonas que deben existir en t_tecnicos y t_repuestos (Flujo 2):", ", ".join(ZONAS))
print("Repuestos usados:", ", ".join(REPUESTOS))
