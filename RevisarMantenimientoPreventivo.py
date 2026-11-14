import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
maquinas = dynamodb.Table(os.environ["TABLA_MAQUINAS"])
mantenimientos = dynamodb.Table(os.environ["TABLA_MANTENIMIENTO"])
resultados = dynamodb.Table(os.environ["TABLA_RESULTADOS"])

def lambda_handler(event, context):
    revisadas = 0
    alertas = 0

    # Escanea el estado de las máquinas. Para una flota grande conviene paginar
    # y/o diseñar un índice según los criterios de consulta.
    pagina = maquinas.scan()
    items = pagina.get("Items", [])

    while "LastEvaluatedKey" in pagina:
        pagina = maquinas.scan(ExclusiveStartKey=pagina["LastEvaluatedKey"])
        items.extend(pagina.get("Items", []))

    for maquina in items:
        machine_id = maquina["machine_id"]
        horas_actuales = float(maquina.get("horas_uso", 0))
        revisadas += 1

        # Se espera que cada registro de mantenimiento contenga:
        # intervalo_horas y horas_uso_mantenimiento.
        respuesta = mantenimientos.query(
            KeyConditionExpression=Key("machine_id").eq(machine_id),
            ScanIndexForward=False,
            Limit=1
        )
        historial = respuesta.get("Items", [])
        if not historial:
            print(f"{machine_id}: sin historial de mantenimiento; se omite.")
            continue

        ultimo = historial[0]
        intervalo = float(ultimo.get("intervalo_horas", 0))
        horas_ultimo = float(ultimo.get("horas_uso_mantenimiento", 0))

        if intervalo <= 0:
            print(f"{machine_id}: intervalo_horas no configurado.")
            continue

        faltan = intervalo - (horas_actuales - horas_ultimo)
        if faltan <= 0:
            fecha = ultimo["fecha"]
            resultados.put_item(Item={
                "machine_id": machine_id,
                "timestamp": fecha,
                "severidad": "PREVENTIVO",
                "motivo": "MANTENIMIENTO_POR_HORAS",
                "horas_uso_actuales": Decimal(str(horas_actuales)),
                "intervalo_horas": Decimal(str(intervalo)),
                "horas_ultimo_mantenimiento": Decimal(str(horas_ultimo))
            })
            alertas += 1
            print(f"{machine_id}: mantenimiento preventivo requerido.")

    return {"maquinas_revisadas": revisadas, "alertas_preventivas": alertas}
