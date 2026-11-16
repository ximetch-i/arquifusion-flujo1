import os, statistics
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

app = FastAPI()
API_KEY = os.environ["API_KEY"]

class Lectura(BaseModel):
    machine_id: str
    timestamp: str
    temperatura: float
    vibracion: float

class Entrada(BaseModel):
    machine_id: str
    ventana: list[Lectura]

@app.get("/salud")
def salud():
    return {"estado": "ok"}

@app.post("/analizar")
def analizar(body: Entrada, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(401, "API key inválida")
    if not body.ventana:
        raise HTTPException(422, "ventana vacía")
    t = statistics.mean(l.temperatura for l in body.ventana)
    v = statistics.mean(l.vibracion for l in body.ventana)
    t_score = min(max((t - 60) / 40, 0), 1)
    v_score = min(max((v - 2) / 6, 0), 1)
    puntaje = round(max(t_score, v_score), 3)
    return {"puntaje_anomalia": puntaje,
            "prob_falla": round(puntaje * 0.95, 3),
            "variables_relevantes": [n for n, s in (("temperatura", t_score), ("vibracion", v_score)) if s > 0.5]}
