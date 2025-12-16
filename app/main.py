from fastapi import FastAPI, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import importlib
import os
import sys
import logging
import time
from dotenv import load_dotenv
from typing import List, Optional
import mysql.connector
from app.database import get_db

# Cargar variables de entorno
load_dotenv("/home/ubuntu/FastAPI_BICICLA/.env")
# Configurar logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "app.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# Crear la app
app = FastAPI(
    title="API de Sensores de Bicicletas",
    description="API para consultar datos de sensores de conteo de bicicletas",
    version="1.0.0"
)
# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Endpoint raíz
@app.get("/")
def read_root():
    return {
        "status": "ok",
        "message": "API de Sensores de Bicicletas funcionando correctamente",
        "version": "1.0.0",
        "documentacion": "/docs"
    }

# Función para importar el módulo MQTT con manejo de reintentos
def get_mqtt_client():
    """Importa el módulo MQTT con recarga para permitir reinicio del servicio"""
    if "app.mqtt_client" in sys.modules:
        del sys.modules["app.mqtt_client"]
    import app.mqtt_client as mqtt_client

    mqtt_client.restart_application = restart_mqtt_service
    return mqtt_client

# MQTT
mqtt_task = None
mqtt_restart_count = 0
MAX_MQTT_RESTARTS = 100

async def restart_mqtt_service():
    """Reinicia solo el servicio MQTT en caso de fallos graves"""
    logging.warning("🔄 REINICIANDO SERVICIO MQTT...")
    await asyncio.sleep(2)
    asyncio.create_task(start_or_restart_mqtt())

async def start_or_restart_mqtt():
    """Inicia o reinicia el cliente MQTT de forma segura"""
    global mqtt_task, mqtt_restart_count

    if mqtt_task and not mqtt_task.done():
        mqtt_task.cancel()
        try:
            await mqtt_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"Error al cancelar tarea MQTT anterior: {e}")

    mqtt_restart_count += 1
    if mqtt_restart_count > MAX_MQTT_RESTARTS:
        logging.critical(f"Se alcanzó el límite de reintentos MQTT ({MAX_MQTT_RESTARTS}). Servicio en pausa.")
        return

    mqtt_client = get_mqtt_client()
    mqtt_task = asyncio.create_task(mqtt_client.start_mqtt_client())
    logging.info(f"🔄 Servicio MQTT (re)iniciado (intento {mqtt_restart_count})")
    print(f"🔄 Servicio MQTT (re)iniciado (intento {mqtt_restart_count})", flush=True)

# Cargar módulos de endpoints
endpoints_path = os.path.join(os.path.dirname(__file__), "endpoints")
os.makedirs(endpoints_path, exist_ok=True)
if os.path.exists(os.path.join(endpoints_path, "mapa.py")):
    try:
        from app.endpoints import mapa
        app.include_router(mapa.router, prefix="/mapa", tags=["Mapa"])
        logging.info("✅ Módulo 'mapa' cargado correctamente")
    except ImportError as e:
        logging.error(f"❌ Error al importar 'mapa': {str(e)}")

for module_name in ["stats", "sensors", "readings", "dashboard"]:
    module_path = os.path.join(endpoints_path, f"{module_name}.py")
    if not os.path.exists(module_path):
        with open(module_path, "w") as f:
            f.write(f"""from fastapi import APIRouter\n\nrouter = APIRouter()\n\n@router.get(\"/test\")\ndef test_endpoint():\n    return {{\"status\": \"ok\", \"message\": \"Endpoint de prueba para {module_name}\"}}\n""")
    try:
        module = importlib.import_module(f"app.endpoints.{module_name}")
        app.include_router(module.router, prefix=f"/{module_name}", tags=[module_name.capitalize()])
        logging.info(f"✅ Módulo '{module_name}' cargado correctamente")
    except Exception as e:
        logging.error(f"❌ Error al cargar módulo '{module_name}': {str(e)}")

# Importar tipos y funciones de readings
if 'app.endpoints.readings' in sys.modules:
    del sys.modules['app.endpoints.readings']

from app.endpoints.readings import LecturaResponse, ResumenResponse, GraficoDetalladoResponse
from app.endpoints.readings import (
    obtener_comunas, obtener_ubicaciones, obtener_sentidos,
    consultar_lecturas, obtener_datos_grafico, obtener_datos_grafico_detallado,
    obtener_resumen
)

# Endpoints de compatibilidad
@app.get("/comunas")
def comunas_compat(db: mysql.connector.connection.MySQLConnection = Depends(get_db)):
    """Endpoint de compatibilidad para obtener comunas"""
    logging.info("Acceso a endpoint /comunas (compatibilidad)")
    return obtener_comunas(db)

@app.get("/ubicaciones")
def ubicaciones_compat(
    comuna_id: Optional[int] = None,
    db: mysql.connector.connection.MySQLConnection = Depends(get_db)
):
    """Endpoint de compatibilidad para obtener ubicaciones"""
    logging.info(f"Acceso a endpoint /ubicaciones (compatibilidad) con comuna_id={comuna_id}")
    return obtener_ubicaciones(comuna_id, db)

@app.get("/sentidos")
def sentidos_compat(
    ubicacion_id: Optional[int] = None,
    db: mysql.connector.connection.MySQLConnection = Depends(get_db)
):
    """Endpoint de compatibilidad para obtener sentidos"""
    logging.info(f"Acceso a endpoint /sentidos (compatibilidad) con ubicacion_id={ubicacion_id}")
    return obtener_sentidos(ubicacion_id, db)

@app.get("/consulta", response_model=LecturaResponse)
def consulta_compat(
    comuna_id: Optional[int] = None,
    ubicacion_id: Optional[int] = None,
    sentidos: Optional[str] = Query(None, description="IDs de sentidos separados por coma"),
    periodo: str = Query("hoy", description="Período: hoy, semana, mes, anio, personalizado"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha de inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha de fin (YYYY-MM-DD)"),
    hora_inicio: Optional[str] = Query(None, description="Hora de inicio (HH:MM)"),
    hora_fin: Optional[str] = Query(None, description="Hora de fin (HH:MM)"),
    db: mysql.connector.connection.MySQLConnection = Depends(get_db)
):
    """Endpoint de compatibilidad para consulta de lecturas"""
    logging.info(f"Acceso a endpoint /consulta (compatibilidad) con periodo={periodo}")
    return consultar_lecturas(
        comuna_id, ubicacion_id, sentidos, periodo,
        fecha_inicio, fecha_fin, hora_inicio, hora_fin, db
    )

@app.get("/grafico", response_model=ResumenResponse)
def grafico_compat(
    comuna_id: Optional[int] = None,
    ubicacion_id: Optional[int] = None,
    sentidos: Optional[str] = Query(None, description="IDs de sentidos separados por coma"),
    periodo: str = Query("hoy", description="Período: hoy, semana, mes, anio, personalizado"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha de inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha de fin (YYYY-MM-DD)"),
    hora_inicio: Optional[str] = Query(None, description="Hora de inicio (HH:MM)"),
    hora_fin: Optional[str] = Query(None, description="Hora de fin (HH:MM)"),
    agrupar_por: str = Query("auto", description="Campo por el cual agrupar: auto, hora, dia, semana, mes"),
    agrupar: bool = Query(False, description="Indica si los datos deben agruparse"),
    db: mysql.connector.connection.MySQLConnection = Depends(get_db)
):
    """Endpoint de compatibilidad para obtener datos agrupados para gráficos"""
    logging.info(f"Acceso a endpoint /grafico (compatibilidad) con periodo={periodo}, agrupar_por={agrupar_por}")
    return obtener_datos_grafico(
        comuna_id, ubicacion_id, sentidos, periodo,
        fecha_inicio, fecha_fin, hora_inicio, hora_fin,
        agrupar_por, agrupar, db
    )

@app.get("/grafico_detallado", response_model=GraficoDetalladoResponse)
def grafico_detallado_compat(
    comuna_id: Optional[int] = None,
    ubicacion_id: Optional[int] = None,
    sentidos: Optional[str] = Query(None, description="IDs de sentidos separados por coma"),
    periodo: str = Query("hoy", description="Período: hoy, semana, mes, anio, personalizado"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha de inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha de fin (YYYY-MM-DD)"),
    hora_inicio: Optional[str] = Query(None, description="Hora de inicio (HH:MM)"),
    hora_fin: Optional[str] = Query(None, description="Hora de fin (HH:MM)"),
    agrupar_por: str = Query("auto", description="Campo por el cual agrupar: auto, hora, dia, semana, mes"),
    agrupar: bool = Query(False, description="Indica si los datos deben agruparse"),
    db: mysql.connector.connection.MySQLConnection = Depends(get_db)
):
    """Endpoint de compatibilidad para obtener datos detallados por sentido para gráficos"""
    logging.info(f"Acceso a endpoint /grafico_detallado (compatibilidad) con periodo={periodo}, agrupar_por={agrupar_por}")
    return obtener_datos_grafico_detallado(
        comuna_id, ubicacion_id, sentidos, periodo,
        fecha_inicio, fecha_fin, hora_inicio, hora_fin,
        agrupar_por, agrupar, db
    )

@app.get("/resumen")
def resumen_compat(db: mysql.connector.connection.MySQLConnection = Depends(get_db)):
    """Endpoint de compatibilidad para obtener el resumen estadístico"""
    logging.info("Acceso a endpoint /resumen (compatibilidad)")
    return obtener_resumen(db)

@app.get("/lecturas")
def lecturas_compat(
    comuna_id: Optional[int] = None,
    ubicacion_id: Optional[int] = None,
    sentidos: Optional[str] = Query(None, description="IDs de sentidos separados por coma"),
    periodo: str = Query("hoy", description="Período: hoy, semana, mes, anio, personalizado"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha de inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha de fin (YYYY-MM-DD)"),
    hora_inicio: Optional[str] = Query(None, description="Hora de inicio (HH:MM)"),
    hora_fin: Optional[str] = Query(None, description="Hora de fin (HH:MM)"),
    db: mysql.connector.connection.MySQLConnection = Depends(get_db)
):
    """Endpoint de compatibilidad para obtener lecturas"""
    try:
        logging.info(f"Acceso a endpoint /lecturas (compatibilidad) con periodo={periodo}")
        result = consultar_lecturas(
            comuna_id, ubicacion_id, sentidos, periodo,
            fecha_inicio, fecha_fin, hora_inicio, hora_fin, db
        )

        lecturas_adaptadas = []

        if result and hasattr(result, 'lecturas') and isinstance(result.lecturas, list):
            for i, lectura in enumerate(result.lecturas):
                try:
                    fecha_partes = lectura.get('fecha', '').split('/')
                    if len(fecha_partes) == 3:
                        fecha_iso = f"{fecha_partes[2]}-{fecha_partes[1]}-{fecha_partes[0]}"
                    else:
                        fecha_iso = lectura.get('fecha', '')

                    fecha_hora_iso = f"{fecha_iso}T{lectura.get('hora', '00:00')}:00"

                    lecturas_adaptadas.append({
                        "id": i + 1,
                        "sensor_id": ubicacion_id or 1,
                        "nombre_sensor": f"Sensor {ubicacion_id or 1}",
                        "ubicacion": lectura.get("ubicacion", ""),
                        "comuna": lectura.get("comuna", ""),
                        "fecha_hora": fecha_hora_iso,
                        "cantidad": lectura.get("cantidad", 1),
                        "sentido": lectura.get("sentido", "")
                    })
                except Exception as e:
                    logging.error(f"Error al procesar lectura individual: {str(e)}")
                    continue
        else:
            if isinstance(result, dict) and 'lecturas' in result:
                for i, lectura in enumerate(result['lecturas']):
                    try:
                        fecha_partes = lectura.get('fecha', '').split('/')
                        if len(fecha_partes) == 3:
                            fecha_iso = f"{fecha_partes[2]}-{fecha_partes[1]}-{fecha_partes[0]}"
                        else:
                            fecha_iso = lectura.get('fecha', '')

                        fecha_hora_iso = f"{fecha_iso}T{lectura.get('hora', '00:00')}:00"

                        lecturas_adaptadas.append({
                            "id": i + 1,
                            "sensor_id": ubicacion_id or 1,
                            "nombre_sensor": f"Sensor {ubicacion_id or 1}",
                            "ubicacion": lectura.get("ubicacion", ""),
                            "comuna": lectura.get("comuna", ""),
                            "fecha_hora": fecha_hora_iso,
                            "cantidad": lectura.get("cantidad", 1),
                            "sentido": lectura.get("sentido", "")
                        })
                    except Exception as e:
                        logging.error(f"Error al procesar lectura individual: {str(e)}")
                        continue

        return lecturas_adaptadas
    except Exception as e:
        import traceback
        logging.error(f"Error en endpoint /lecturas: {str(e)}")
        logging.error(traceback.format_exc())
        return []

# Startup y shutdown
@app.on_event("startup")
async def startup_event():
    global mqtt_restart_count
    mqtt_restart_count = 0

    mqtt_broker = os.getenv("MQTT_BROKER")
    if mqtt_broker:
        await start_or_restart_mqtt()
    else:
        logging.warning("⚠️ No se encontró configuración MQTT válida.")
        print("⚠️ No se encontró configuración MQTT válida.", flush=True)

@app.on_event("shutdown")
async def shutdown_event():
    global mqtt_task
    if mqtt_task:
        mqtt_task.cancel()
        try:
            await mqtt_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"Error al detener MQTT: {e}")
        logging.info("🛑 Servicio MQTT detenido")
        print("🛑 Servicio MQTT detenido", flush=True)

# Entry point
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
