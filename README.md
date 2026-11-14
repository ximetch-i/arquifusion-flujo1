# ArquiFusion Corp — Flujo 1: Prevención de fallas

Implementación inicial basada en `arquitectura_flujo1.drawio`.

## Lambdas (nombres exactos del diagrama)

1. `GenerarLecturaMaquina`: EventBridge cada minuto → consulta/actualiza `t_maquinas` → publica en `TemaLecturas`.
2. `IngestarLectura`: trigger de `ColaLecturas` → valida → `PutObject` en S3 → actualiza `t_maquinas` → `StartExecution` de Step Functions.
3. `PrepararVentana`: recupera de S3 lecturas de los últimos 10 minutos → llama `POST /analizar` en Cloud Run.
4. `ClasificarMaquina`: guarda en `t_resultados`, busca alertas cercanas y publica urgentes en `TemaIncidentes`.
5. `RevisarMantenimientoPreventivo`: EventBridge diario → compara horas actuales con el último mantenimiento.

## Recursos

- SNS: `TemaLecturas`, `TemaIncidentes`
- SQS: `ColaLecturas`, `ColaLecturasDLQ`
- S3: bucket de historial de lecturas
- DynamoDB: `t_maquinas`, `t_resultados`, `t_mantenimiento`
- Step Functions: `FlujoAnalisisMaquina`
- SSM Parameter Store: `/hackathon/gcp-key`

Los nombres físicos de las tablas llevan el sufijo de stage, por ejemplo `t_maquinas-dev`.

## Requisitos

- Node.js y npm
- Serverless Framework v3
- Python 3.12
- AWS CLI configurado con credenciales autorizadas
- Permisos para crear Lambda, SNS, SQS, S3, DynamoDB, Step Functions, IAM y EventBridge

Instalación:

```bash
npm install
npm install --save-dev serverless-step-functions
```

Despliegue:

```bash
npx serverless print
npx serverless deploy
```

Eliminar recursos:

```bash
npx serverless remove
```

## Configuración de Google Cloud

La URL del servicio Cloud Run no es secreta. Antes de desplegar con IA real:

```bash
export GCP_API_URL="https://TU-SERVICIO.run.app"
export AI_MODE="GCP"
```

Guarda la API key en Parameter Store como SecureString:

```bash
aws ssm put-parameter \
  --name "/hackathon/gcp-key" \
  --type "SecureString" \
  --value "TU_API_KEY" \
  --overwrite
```

El código envía la key en el header `x-api-key` y hace `POST {GCP_API_URL}/analizar`.
El contrato esperado de Cloud Run es JSON de entrada:

```json
{
  "machine_id": "MAQ-001",
  "ventana": [
    {
      "machine_id": "MAQ-001",
      "timestamp": "2026-11-14T10:30:00+00:00",
      "temperatura": 80.5,
      "vibracion": 4.1
    }
  ]
}
```

Y JSON de salida:

```json
{
  "puntaje_anomalia": 0.72,
  "prob_falla": 0.68,
  "variables_relevantes": ["temperatura", "vibracion"]
}
```

Mientras Cloud Run no esté listo, `AI_MODE=MOCK` mantiene el flujo ejecutable con una respuesta simulada. El modo MOCK no es un modelo predictivo real.

## Datos de mantenimiento

Para que `RevisarMantenimientoPreventivo` pueda comparar horas, cada máquina debe tener al menos un registro en `t_mantenimiento` con:

- `machine_id` (partition key)
- `fecha` (sort key, formato ISO)
- `intervalo_horas` (horas entre mantenimientos)
- `horas_uso_mantenimiento` (contador de horas cuando se hizo el último mantenimiento)

Ejemplo de item:

```json
{
  "machine_id": "MAQ-001",
  "fecha": "2026-10-01T09:00:00Z",
  "intervalo_horas": 250,
  "horas_uso_mantenimiento": 900
}
```

Sin historial, la Lambda omite esa máquina; no inventa un intervalo.

## Decisiones y puntos que deben ajustarse

- `MAQUINAS` contiene máquinas y coordenadas de demostración. Sustitúyanse por las del caso.
- `combustible` conserva el nombre del esquema recibido; definan si significa nivel o consumo y su unidad antes de entrenar el modelo.
- Los umbrales `UMBRAL_PREVENTIVO` y `UMBRAL_URGENTE` son valores iniciales configurables, no umbrales validados. Deben calibrarse con el historial simulado/etiquetado.
- La detección de causa común solo señala máquinas con alertas próximas por coordenadas; no demuestra causalidad.
- `ClasificarMaquina` hace un `Scan` simple de `t_resultados` para el prototipo. Para producción, conviene agregar índices y limitar la consulta a un rango temporal.
- `IngestarLectura` procesa un mensaje por invocación (`batchSize: 1`) para que los fallos se puedan reintentar individualmente.
- La DLQ se configura en la cola principal con `maxReceiveCount: 3`.
- El bucket S3 tiene nombre único por cuenta y stage. Si el nombre ya existe, despliegue fallará: cambia el prefijo del bucket.

## Pruebas rápidas

1. Desplegar y revisar que EventBridge active `GenerarLecturaMaquina`.
2. Verificar mensajes en `TemaLecturas`/`ColaLecturas`.
3. Verificar objetos en S3 y último estado en `t_maquinas-dev`.
4. Verificar ejecuciones de `FlujoAnalisisMaquina`.
5. Verificar resultados en `t_resultados-dev`.
6. Para probar DLQ, provocar deliberadamente un error de procesamiento en entorno de prueba y comprobar que el mensaje termina en `ColaLecturasDLQ`.

## Nota sobre permisos

AWS Academy Learner Lab puede restringir recursos IAM o servicios. `serverless print` valida la configuración local, pero no garantiza que la cuenta permita desplegar todos los recursos.
