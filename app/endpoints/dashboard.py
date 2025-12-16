from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
import logging
import mysql.connector
from mysql.connector.connection import MySQLConnection

# Importar nuestra conexión a la base de datos
from app.database import get_db

# Crear router para los endpoints de dashboard
router = APIRouter()

# Endpoint de prueba básico
@router.get("/test")
def test_endpoint():
    return {"status": "ok", "message": "Endpoint de prueba para dashboard"}

@router.get("/summary")
def get_dashboard_summary(
    conn: MySQLConnection = Depends(get_db)
):
    """
    Obtiene un resumen de datos para el dashboard principal.
    Incluye conteos actuales, estados de sensores y estadísticas generales.
    """
    cursor = conn.cursor(dictionary=True)
    try:
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        first_day_current_month = today.replace(day=1)
        
        # 1. Conteo total de hoy
        cursor.execute(
            "SELECT COUNT(*) as total FROM LECTURAS WHERE DATE(FECHA_LECTURA) = %s",
            (today,)
        )
        total_today = cursor.fetchone()['total'] or 0
        
        # 2. Conteo total de ayer (para calcular variación)
        cursor.execute(
            "SELECT COUNT(*) as total FROM LECTURAS WHERE DATE(FECHA_LECTURA) = %s",
            (yesterday,)
        )
        total_yesterday = cursor.fetchone()['total'] or 0
        
        # Calcular variación porcentual diaria
        if total_yesterday > 0:
            variacion_diaria = ((total_today - total_yesterday) / total_yesterday) * 100
        else:
            variacion_diaria = 0
        
        # 3. Promedio diario de la última semana
        start_of_week = today - timedelta(days=7)
        cursor.execute("""
            SELECT DATE(FECHA_LECTURA) as fecha, COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) >= %s AND DATE(FECHA_LECTURA) < %s
            GROUP BY DATE(FECHA_LECTURA)
        """, (start_of_week, today))
        
        results_week = cursor.fetchall()
        
        if results_week:
            total_dias = len(results_week)
            total_ciclistas_semana = sum(r['total'] for r in results_week)
            promedio_semanal = total_ciclistas_semana / total_dias
        else:
            promedio_semanal = 0
        
        # 4. Promedio diario de la semana anterior (para calcular variación)
        start_of_prev_week = start_of_week - timedelta(days=7)
        cursor.execute("""
            SELECT DATE(FECHA_LECTURA) as fecha, COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) >= %s AND DATE(FECHA_LECTURA) < %s
            GROUP BY DATE(FECHA_LECTURA)
        """, (start_of_prev_week, start_of_week))
        
        results_prev_week = cursor.fetchall()
        
        if results_prev_week:
            total_dias_prev = len(results_prev_week)
            total_ciclistas_semana_prev = sum(r['total'] for r in results_prev_week)
            promedio_semanal_prev = total_ciclistas_semana_prev / total_dias_prev
        else:
            promedio_semanal_prev = 0
        
        # Calcular variación porcentual semanal
        if promedio_semanal_prev > 0:
            variacion_semanal = ((promedio_semanal - promedio_semanal_prev) / promedio_semanal_prev) * 100
        else:
            variacion_semanal = 0
        
        # 5. Conteo total del mes actual
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) >= %s AND DATE(FECHA_LECTURA) <= %s
        """, (first_day_current_month, today))
        
        total_current_month = cursor.fetchone()['total'] or 0
        
        # 6. Conteo total del mes anterior (para calcular variación)
        last_day_prev_month = first_day_current_month - timedelta(days=1)
        first_day_prev_month = last_day_prev_month.replace(day=1)
        
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) >= %s AND DATE(FECHA_LECTURA) <= %s
        """, (first_day_prev_month, last_day_prev_month))
        
        total_prev_month = cursor.fetchone()['total'] or 0
        
        # Calcular variación porcentual mensual
        if total_prev_month > 0:
            variacion_mensual = ((total_current_month - total_prev_month) / total_prev_month) * 100
        else:
            variacion_mensual = 0
        
        # 7. Conteo de sensores
        cursor.execute("""
            SELECT COUNT(DISTINCT NOMBRE_SENSOR) as total
            FROM SENSORES
        """)
        total_sensores = cursor.fetchone()['total'] or 0
        
        # Sensores activos (con lecturas en las últimas 3 horas)
        three_hours_ago = datetime.now() - timedelta(hours=3)
        
        # Usar NOMBRE_SENSOR en lugar de ID_SENSOR
        query_activos = """
            SELECT COUNT(DISTINCT s.NOMBRE_SENSOR) as total
            FROM SENSORES s
            JOIN LECTURAS l ON s.NOMBRE_SENSOR = l.NOMBRE_SENSOR
            WHERE l.FECHA_LECTURA >= %s
        """
        
        cursor.execute(query_activos, (three_hours_ago,))
        sensores_activos = cursor.fetchone()['total'] or 0
        
        # Sensores inactivos
        sensores_inactivos = total_sensores - sensores_activos
        
        # 8. Obtener datos para el gráfico de tendencia semanal
        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        
        cursor.execute("""
            SELECT DATE(FECHA_LECTURA) as fecha, COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) >= %s AND DATE(FECHA_LECTURA) <= %s
            GROUP BY DATE(FECHA_LECTURA)
        """, (start_of_week, end_of_week))
        
        results = cursor.fetchall()
        
        # Crear un diccionario con todas las fechas de la semana
        dias_semana = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
        datos_diarios = []
        
        for i in range(7):
            fecha = start_of_week + timedelta(days=i)
            # Buscar si hay datos para esta fecha
            total = 0
            for r in results:
                if r['fecha'] == fecha:
                    total = r['total']
                    break
            
            datos_diarios.append({
                "dia": dias_semana[i],
                "fecha": fecha.isoformat(),
                "total": total
            })
        
        # 9. Obtener las comunas con más ciclistas
        cursor.execute("""
            SELECT COMUNA, COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) = %s
            GROUP BY COMUNA
            ORDER BY total DESC
            LIMIT 5
        """, (today,))
        
        top_comunas = cursor.fetchall()
        
        # 10. Obtener los sensores más activos
        query_sensores_top = """
            SELECT 
                s.NOMBRE_SENSOR as nombre,
                COUNT(l.FECHA_LECTURA) as conteo_hoy
            FROM SENSORES s
            LEFT JOIN LECTURAS l ON s.NOMBRE_SENSOR = l.NOMBRE_SENSOR AND DATE(l.FECHA_LECTURA) = %s
            GROUP BY s.NOMBRE_SENSOR
            ORDER BY conteo_hoy DESC
            LIMIT 5
        """
        
        cursor.execute(query_sensores_top, (today,))
        sensores_top_result = cursor.fetchall()
        
        # Determinar el estado de cada sensor top
        sensores_top = []
        for sensor in sensores_top_result:
            nombre_sensor = sensor['nombre']
            
            # Obtener la última lectura del sensor
            ultima_lectura_query = """
                SELECT MAX(FECHA_LECTURA) as ultima_fecha
                FROM LECTURAS
                WHERE NOMBRE_SENSOR = %s
            """
            cursor.execute(ultima_lectura_query, (nombre_sensor,))
            ultima_lectura_result = cursor.fetchone()
            ultima_lectura = ultima_lectura_result['ultima_fecha'] if ultima_lectura_result and ultima_lectura_result['ultima_fecha'] else None
            
            # Determinar el estado del sensor
            estado_sensor = "active"
            
            if not ultima_lectura or (datetime.now() - ultima_lectura).total_seconds() > 3600 * 3:
                estado_sensor = "inactive"
            elif sensor['conteo_hoy'] < 10:  # Umbral bajo de lecturas
                estado_sensor = "warning"
            
            # Generar un ID basado en el nombre del sensor (ya que ID_SENSOR podría no estar disponible)
            sensor_id = hash(nombre_sensor) % 10000  # Usar hash para generar un ID único
            
            sensores_top.append({
                "id": sensor_id,
                "nombre": nombre_sensor,
                "conteo_hoy": sensor['conteo_hoy'],
                "estado": estado_sensor
            })
        
        # Resultado final
        return {
            "ciclistas_hoy": {
                "total": total_today,
                "variacion_porcentual": round(variacion_diaria, 1)
            },
            "promedio_diario": {
                "total": round(promedio_semanal, 1),
                "variacion_porcentual": round(variacion_semanal, 1)
            },
            "ciclistas_mes": {
                "total": total_current_month,
                "variacion_porcentual": round(variacion_mensual, 1)
            },
            "sensores": {
                "total": total_sensores,
                "activos": sensores_activos,
                "inactivos": sensores_inactivos
            },
            "tendencia_semanal": {
                "periodo": "semana actual",
                "datos": datos_diarios
            },
            "top_comunas": top_comunas,
            "sensores_top": sensores_top
        }
    
    except mysql.connector.Error as err:
        logging.error(f"Error de base de datos en get_dashboard_summary: {err}")
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(err)}")
    finally:
        cursor.close()
