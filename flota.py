"""Flota simulada del Flujo 1: UNICA fuente de verdad.

La usan simulador.py (para generar lecturas) y seed_flota.py (para cargar t_maquinas).
La zona NO viaja en la lectura del sensor (solo lat/lon): vive en t_maquinas.
Sustituyan zonas, coordenadas y valores base por los del caso real.
"""
import os
import random

NUM_MAQUINAS = int(os.environ.get("NUM_MAQUINAS", "30"))

# Centro aproximado de cada zona. Las zonas deben coincidir con las de t_tecnicos / t_repuestos (Flujo 2).
ZONAS = {
    "Lima Norte": (-11.9500, -77.0700),
    "Lima Centro": (-12.0464, -77.0428),
    "Lima Sur": (-12.1500, -76.9900),
}

# Valores base de las 3 primeras máquinas (los del repo original).
BASE_ORIGINAL = [
    {"temperatura": 75.0, "vibracion": 3.0, "presion": 150.0, "rpm": 1600, "combustible": 12.0, "carga": 8.0, "velocidad": 15.0},
    {"temperatura": 80.0, "vibracion": 4.0, "presion": 160.0, "rpm": 1800, "combustible": 15.0, "carga": 10.0, "velocidad": 18.0},
    {"temperatura": 70.0, "vibracion": 2.5, "presion": 145.0, "rpm": 1500, "combustible": 10.0, "carga": 6.0, "velocidad": 12.0},
]


def construir_flota():
    rng = random.Random(42)  # semilla fija: la flota es idéntica en cada arranque
    nombres_zona = list(ZONAS)
    flota = []
    for i in range(1, NUM_MAQUINAS + 1):
        zona = nombres_zona[(i - 1) % len(nombres_zona)]
        lat0, lon0 = ZONAS[zona]
        if i <= len(BASE_ORIGINAL):
            sensores = dict(BASE_ORIGINAL[i - 1])
        else:
            sensores = {
                "temperatura": round(rng.uniform(68, 82), 1),
                "vibracion": round(rng.uniform(2.2, 4.2), 2),
                "presion": round(rng.uniform(140, 165), 1),
                "rpm": rng.randint(1450, 1850),
                "combustible": round(rng.uniform(9, 16), 1),
                "carga": round(rng.uniform(5, 11), 1),
                "velocidad": round(rng.uniform(11, 19), 1),
            }
        flota.append({
            "machine_id": f"MAQ-{i:03d}",
            "zona": zona,
            # ±0.03° ≈ ±3 km: las máquinas de una misma zona quedan dentro del radio de "causa común"
            "lat": round(lat0 + rng.uniform(-0.03, 0.03), 4),
            "lon": round(lon0 + rng.uniform(-0.03, 0.03), 4),
            **sensores,
        })
    return flota
