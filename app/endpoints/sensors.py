from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from datetime import datetime
import logging
import mysql.connector
from mysql.connector.connection import MySQLConnection
from pydantic import BaseModel

from app.database import get_db

router = APIRouter()

class SensorUpdate(BaseModel):
    nombre_sensor_formal: str
    tipo_equipo: str
    sentido_lectura: str
    nombre_ubicacion: str
    comuna: str
    estado: str
    usuario: Optional[str] = "sistema"

@router.get("/list")
def get_sensors(
    ubicacion_id: Optional[int] = None,
    estado: Optional[str] = None,
    conn: MySQLConnection = Depends(get_db)
):
    cursor = conn.cursor(dictionary=True)
    try:
        today = datetime.now().date()
        query = """
        SELECT vsu.*, e.DESCRIPCION_ESTADO
        FROM BICICLA.VISTA_SENSORES_UBICACIONES vsu
        LEFT JOIN BICICLA.ESTADOS e ON vsu.ESTADO_SENSOR = e.CODIGO_ESTADO
        """

        if ubicacion_id:
            query += " WHERE vsu.ID_UBICACION = %s"
            cursor.execute(query, (ubicacion_id,))
        else:
            cursor.execute(query)

        sensores = cursor.fetchall()
        result = []

        for sensor in sensores:
            nombre_sensor = sensor.get('NOMBRE_SENSOR')
            if not nombre_sensor:
                continue

            cursor.execute("""
                SELECT COUNT(*) as conteo
                FROM LECTURAS
                WHERE NOMBRE_SENSOR = %s AND DATE(FECHA_LECTURA) = %s
            """, (nombre_sensor, today))
            conteo_hoy = cursor.fetchone()['conteo']

            cursor.execute("""
                SELECT MAX(FECHA_LECTURA) as ultima_fecha
                FROM LECTURAS
                WHERE NOMBRE_SENSOR = %s
            """, (nombre_sensor,))
            ultima_lectura = cursor.fetchone()['ultima_fecha']

            result.append({
                "id": sensor.get('ID_SENSOR'),
                "nombre_sensor_formal": sensor.get('NOMBRE_SENSOR_FORMAL', ''),
                "ubicacion": {
                    "id": sensor.get('ID_UBICACION'),
                    "comuna": sensor.get('COMUNA'),
                    "nombre": sensor.get('NOMBRE_UBICACION'),
                    "tipo_equipo": sensor.get('TIPO_EQUIPO')
                },
                "latitud": float(sensor.get('LAT_SENSOR', 0)) if sensor.get('LAT_SENSOR') else 0,
                "longitud": float(sensor.get('LNG_SENSOR', 0)) if sensor.get('LNG_SENSOR') else 0,
                "sentido_lectura": sensor.get('SENTIDO_LECTURA'),
                "conteo_hoy": conteo_hoy,
                "estado": {
                    "codigo": sensor.get('ESTADO_SENSOR', 'unknown'),
                    "descripcion": sensor.get('DESCRIPCION_ESTADO', 'Desconocido')
                },
                "ultima_lectura": ultima_lectura.isoformat() if ultima_lectura else None
            })

        return result

    except mysql.connector.Error as err:
        logging.error(f"Error de base de datos en get_sensors: {err}")
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(err)}")
    finally:
        cursor.close()

@router.get("/map")
def get_map_sensors(conn: MySQLConnection = Depends(get_db)):
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT vsu.*, e.DESCRIPCION_ESTADO
            FROM BICICLA.VISTA_SENSORES_UBICACIONES vsu
            LEFT JOIN BICICLA.ESTADOS e ON vsu.ESTADO_SENSOR = e.CODIGO_ESTADO
        """)
        sensores = cursor.fetchall()

        result = []
        for sensor in sensores:
            if sensor["LAT_SENSOR"] is None or sensor["LNG_SENSOR"] is None:
                continue

            result.append({
                "id": sensor["ID_SENSOR"],
                "nombre_sensor_formal": sensor.get("NOMBRE_SENSOR_FORMAL", ""),
                "ubicacion": {
                    "id": sensor.get("ID_UBICACION"),
                    "comuna": sensor.get("COMUNA"),
                    "nombre": sensor.get("NOMBRE_UBICACION"),
                    "tipo_equipo": sensor.get("TIPO_EQUIPO")
                },
                "latitud": float(sensor["LAT_SENSOR"]),
                "longitud": float(sensor["LNG_SENSOR"]),
                "sentido_lectura": sensor.get("SENTIDO_LECTURA"),
                "estado": {
                    "codigo": sensor.get("ESTADO_SENSOR", "unknown"),
                    "descripcion": sensor.get("DESCRIPCION_ESTADO", "Desconocido")
                }
            })

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()

@router.get("/detail/{sensor_id}")
def get_sensor_detail(sensor_id: int, conn: MySQLConnection = Depends(get_db)):
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT vsu.*, e.DESCRIPCION_ESTADO
            FROM BICICLA.VISTA_SENSORES_UBICACIONES vsu
            LEFT JOIN BICICLA.ESTADOS e ON vsu.ESTADO_SENSOR = e.CODIGO_ESTADO
            WHERE vsu.ID_SENSOR = %s
        """, (sensor_id,))
        rows = cursor.fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail=f"Sensor ID {sensor_id} no encontrado")

        sensor = rows[0]

        return {
            "id": sensor["ID_SENSOR"],
            "nombre_sensor_formal": sensor.get("NOMBRE_SENSOR_FORMAL", ""),
            "estado": {
                "codigo": sensor.get("ESTADO_SENSOR", "unknown"),
                "descripcion": sensor.get("DESCRIPCION_ESTADO", "Desconocido")
            },
            "ubicacion": {
                "id": sensor.get("ID_UBICACION"),
                "comuna": sensor.get("COMUNA"),
                "nombre": sensor.get("NOMBRE_UBICACION"),
                "tipo_equipo": sensor.get("TIPO_EQUIPO")
            },
            "latitud": float(sensor.get("LAT_SENSOR", 0)),
            "longitud": float(sensor.get("LNG_SENSOR", 0)),
            "sentidos": [
                {
                    "id_sentido": row.get("ID_SENTIDO"),
                    "direccion": row.get("DIRECCION"),
                    "sentido_lectura": row.get("SENTIDO_LECTURA")
                }
                for row in rows if row.get("ID_SENTIDO") is not None
            ]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()

@router.get("/auditoria/{sensor_id}")
def get_sensor_auditoria(sensor_id: int, conn: MySQLConnection = Depends(get_db)):
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT ID_CAMBIO, CAMPO_MODIFICADO, VALOR_ANTERIOR, VALOR_NUEVO, USUARIO, FECHA_CAMBIO
            FROM BICICLA.CAMBIOS_SENSORES
            WHERE ID_SENSOR = %s
            ORDER BY FECHA_CAMBIO DESC
        """, (sensor_id,))
        return cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()

@router.get("/estados")
def get_estados(conn: MySQLConnection = Depends(get_db)):
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT CODIGO_ESTADO as codigo, DESCRIPCION_ESTADO as descripcion FROM BICICLA.ESTADOS")
        return cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()

@router.put("/update/{sensor_id}")
def update_sensor(sensor_id: int, data: SensorUpdate, conn: MySQLConnection = Depends(get_db)):
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT 1 FROM BICICLA.ESTADOS WHERE CODIGO_ESTADO = %s", (data.estado,))
        if cursor.fetchone() is None:
            raise HTTPException(status_code=400, detail="Estado no válido")

        cursor.execute("SELECT * FROM BICICLA.SENSORES WHERE ID_SENSOR = %s", (sensor_id,))
        original = cursor.fetchone()
        if not original:
            raise HTTPException(status_code=404, detail="Sensor no encontrado")

        cursor.execute("SELECT * FROM BICICLA.UBICACIONES WHERE ID_UBICACION = %s", (original['ID_UBICACION'],))
        ubicacion_original = cursor.fetchone()

        cursor.execute("""
            UPDATE BICICLA.SENSORES SET
                NOMBRE_FORMAL = %s,
                SENTIDO_LECTURA = %s,
                ESTADO_SENSOR = %s
            WHERE ID_SENSOR = %s
        """, (data.nombre_sensor_formal, data.sentido_lectura, data.estado, sensor_id))

        cursor.execute("""
            UPDATE BICICLA.UBICACIONES SET
                NOMBRE_FORMAL = %s,
                COMUNA = %s,
                TIPO_EQUIPO = %s
            WHERE ID_UBICACION = %s
        """, (data.nombre_ubicacion, data.comuna, data.tipo_equipo, original['ID_UBICACION']))

        def log_change(field, old, new):
            if old is not None and str(old).strip() != str(new).strip():
                logging.info(f"Cambio detectado en {field}: '{old}' => '{new}'")
                try:
                    cursor.execute("""
                        INSERT INTO BICICLA.CAMBIOS_SENSORES
                        (ID_SENSOR, CAMPO_MODIFICADO, VALOR_ANTERIOR, VALOR_NUEVO, USUARIO, FECHA_CAMBIO)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                    """, (sensor_id, field, old, new, data.usuario))
                except Exception as e:
                    logging.warning(f"Error insertando log de cambio en campo {field}: {e}")

        log_change("NOMBRE_FORMAL", original['NOMBRE_FORMAL'], data.nombre_sensor_formal)
        log_change("SENTIDO_LECTURA", original['SENTIDO_LECTURA'], data.sentido_lectura)
        log_change("ESTADO_SENSOR", original['ESTADO_SENSOR'], data.estado)
        log_change("NOMBRE_UBICACION", ubicacion_original['NOMBRE_FORMAL'], data.nombre_ubicacion)
        log_change("COMUNA", ubicacion_original['COMUNA'], data.comuna)
        log_change("TIPO_EQUIPO", ubicacion_original['TIPO_EQUIPO'], data.tipo_equipo)

        conn.commit()
        return {"status": "ok", "message": "Sensor actualizado correctamente"}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
