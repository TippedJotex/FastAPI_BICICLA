from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from datetime import datetime, date, timedelta
import mysql.connector
from mysql.connector.connection import MySQLConnection

# Importar nuestra conexión a la base de datos
from app.database import get_db

# Crear router para los endpoints de estadísticas
router = APIRouter()

# Modelo de respuesta para estadísticas diarias
class StatsToday:
    def __init__(self, total_ciclistas: int, variacion_porcentual: float, fecha: date):
        self.total_ciclistas = total_ciclistas
        self.variacion_porcentual = variacion_porcentual
        self.fecha = fecha

# Modelo de respuesta para promedio diario
class StatsDailyAverage:
    def __init__(self, promedio_diario: float, variacion_porcentual: float, periodo: str):
        self.promedio_diario = promedio_diario
        self.variacion_porcentual = variacion_porcentual
        self.periodo = periodo

# Modelo de respuesta para estadísticas mensuales
class StatsMonthly:
    def __init__(self, total_ciclistas: int, variacion_porcentual: float, mes: str, anio: int):
        self.total_ciclistas = total_ciclistas
        self.variacion_porcentual = variacion_porcentual
        self.mes = mes
        self.anio = anio

# Modelo para datos diarios del gráfico
class DailyData:
    def __init__(self, dia: str, fecha: date, total: int):
        self.dia = dia
        self.fecha = fecha
        self.total = total

# Modelo para la tendencia semanal
class WeeklyTrend:
    def __init__(self, periodo: str, datos: List[DailyData]):
        self.periodo = periodo
        self.datos = datos

# Endpoints para estadísticas generales
@router.get("/today", response_model=dict)
def get_stats_today(
    ubicacion_id: Optional[int] = None,
    sensor_id: Optional[int] = None,
    conn: MySQLConnection = Depends(get_db)
):
    """
    Obtiene el conteo total de ciclistas para el día actual.
    Opcionalmente filtra por ubicación o sensor específico.
    """
    cursor = conn.cursor(dictionary=True)
    try:
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        # Consulta para el total de hoy
        query_today = """
            SELECT COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) = %s
        """
        params_today = [today]
        
        if ubicacion_id:
            query_today += " AND ID_UBICACION = %s"
            params_today.append(ubicacion_id)
        
        if sensor_id:
            query_today += " AND ID_SENSOR = %s"
            params_today.append(sensor_id)
        
        cursor.execute(query_today, params_today)
        result_today = cursor.fetchone()
        total_today = result_today['total'] if result_today else 0
        
        # Consulta para el total de ayer
        query_yesterday = """
            SELECT COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) = %s
        """
        params_yesterday = [yesterday]
        
        if ubicacion_id:
            query_yesterday += " AND ID_UBICACION = %s"
            params_yesterday.append(ubicacion_id)
        
        if sensor_id:
            query_yesterday += " AND ID_SENSOR = %s"
            params_yesterday.append(sensor_id)
        
        cursor.execute(query_yesterday, params_yesterday)
        result_yesterday = cursor.fetchone()
        total_yesterday = result_yesterday['total'] if result_yesterday else 0
        
        # Calcular variación porcentual
        if total_yesterday > 0:
            variacion = ((total_today - total_yesterday) / total_yesterday) * 100
        else:
            variacion = 0
        
        # Retornar respuesta formateada
        return {
            "total_ciclistas": total_today,
            "variacion_porcentual": round(variacion, 1),
            "fecha": today.isoformat()
        }
        
    finally:
        cursor.close()

@router.get("/daily-average", response_model=dict)
def get_stats_daily_average(
    periodo: str = "semana",
    ubicacion_id: Optional[int] = None,
    sensor_id: Optional[int] = None,
    conn: MySQLConnection = Depends(get_db)
):
    """
    Obtiene el promedio diario de ciclistas para un período especificado.
    Períodos disponibles: 'semana' o 'mes'.
    Opcionalmente filtra por ubicación o sensor específico.
    """
    cursor = conn.cursor(dictionary=True)
    try:
        today = datetime.now().date()
        
        # Definir períodos
        if periodo == "semana":
            start_date = today - timedelta(days=7)
            prev_start_date = start_date - timedelta(days=7)
        elif periodo == "mes":
            start_date = today.replace(day=1)
            last_day_prev_month = start_date - timedelta(days=1)
            prev_start_date = last_day_prev_month.replace(day=1)
        else:
            raise HTTPException(status_code=400, detail="Período no válido. Use 'semana' o 'mes'.")
        
        # Consulta para el promedio del período actual
        query_current = """
            SELECT DATE(FECHA_LECTURA) as fecha, COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) >= %s AND DATE(FECHA_LECTURA) <= %s
        """
        params_current = [start_date, today]
        
        if ubicacion_id:
            query_current += " AND ID_UBICACION = %s"
            params_current.append(ubicacion_id)
        
        if sensor_id:
            query_current += " AND ID_SENSOR = %s"
            params_current.append(sensor_id)
        
        query_current += " GROUP BY DATE(FECHA_LECTURA)"
        
        cursor.execute(query_current, params_current)
        results_current = cursor.fetchall()
        
        if results_current:
            total_dias = len(results_current)
            total_ciclistas = sum(r['total'] for r in results_current)
            promedio_actual = total_ciclistas / total_dias
        else:
            promedio_actual = 0
        
        # Consulta para el promedio del período anterior
        query_prev = """
            SELECT DATE(FECHA_LECTURA) as fecha, COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) >= %s AND DATE(FECHA_LECTURA) < %s
        """
        params_prev = [prev_start_date, start_date]
        
        if ubicacion_id:
            query_prev += " AND ID_UBICACION = %s"
            params_prev.append(ubicacion_id)
        
        if sensor_id:
            query_prev += " AND ID_SENSOR = %s"
            params_prev.append(sensor_id)
        
        query_prev += " GROUP BY DATE(FECHA_LECTURA)"
        
        cursor.execute(query_prev, params_prev)
        results_prev = cursor.fetchall()
        
        if results_prev:
            total_dias_prev = len(results_prev)
            total_ciclistas_prev = sum(r['total'] for r in results_prev)
            promedio_anterior = total_ciclistas_prev / total_dias_prev
        else:
            promedio_anterior = 0
        
        # Calcular variación porcentual
        if promedio_anterior > 0:
            variacion = ((promedio_actual - promedio_anterior) / promedio_anterior) * 100
        else:
            variacion = 0
        
        # Retornar respuesta formateada
        return {
            "promedio_diario": round(promedio_actual, 1),
            "variacion_porcentual": round(variacion, 1),
            "periodo": periodo
        }
        
    finally:
        cursor.close()

@router.get("/monthly", response_model=dict)
def get_stats_monthly(
    ubicacion_id: Optional[int] = None,
    sensor_id: Optional[int] = None,
    conn: MySQLConnection = Depends(get_db)
):
    """
    Obtiene el conteo total de ciclistas para el mes actual.
    Opcionalmente filtra por ubicación o sensor específico.
    """
    cursor = conn.cursor(dictionary=True)
    try:
        today = datetime.now().date()
        first_day_current_month = today.replace(day=1)
        last_day_prev_month = first_day_current_month - timedelta(days=1)
        first_day_prev_month = last_day_prev_month.replace(day=1)
        
        # Consulta para el total del mes actual
        query_current = """
            SELECT COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) >= %s AND DATE(FECHA_LECTURA) <= %s
        """
        params_current = [first_day_current_month, today]
        
        if ubicacion_id:
            query_current += " AND ID_UBICACION = %s"
            params_current.append(ubicacion_id)
        
        if sensor_id:
            query_current += " AND ID_SENSOR = %s"
            params_current.append(sensor_id)
        
        cursor.execute(query_current, params_current)
        result_current = cursor.fetchone()
        total_current = result_current['total'] if result_current else 0
        
        # Consulta para el total del mes anterior
        query_prev = """
            SELECT COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) >= %s AND DATE(FECHA_LECTURA) <= %s
        """
        params_prev = [first_day_prev_month, last_day_prev_month]
        
        if ubicacion_id:
            query_prev += " AND ID_UBICACION = %s"
            params_prev.append(ubicacion_id)
        
        if sensor_id:
            query_prev += " AND ID_SENSOR = %s"
            params_prev.append(sensor_id)
        
        cursor.execute(query_prev, params_prev)
        result_prev = cursor.fetchone()
        total_prev = result_prev['total'] if result_prev else 0
        
        # Calcular variación porcentual
        if total_prev > 0:
            variacion = ((total_current - total_prev) / total_prev) * 100
        else:
            variacion = 0
        
        # Obtener el nombre del mes en español
        meses = [
            "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
            "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
        ]
        
        # Retornar respuesta formateada
        return {
            "total_ciclistas": total_current,
            "variacion_porcentual": round(variacion, 1),
            "mes": meses[today.month - 1],
            "anio": today.year
        }
        
    finally:
        cursor.close()

@router.get("/weekly-trend", response_model=dict)
def get_weekly_trend(
    ubicacion_id: Optional[int] = None,
    sensor_id: Optional[int] = None,
    conn: MySQLConnection = Depends(get_db)
):
    """
    Obtiene los datos para el gráfico de tendencia semanal.
    Devuelve el conteo de ciclistas para cada día de la semana actual.
    Opcionalmente filtra por ubicación o sensor específico.
    """
    cursor = conn.cursor(dictionary=True)
    try:
        today = datetime.now().date()
        start_of_week = today - timedelta(days=today.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        
        # Consulta para obtener datos diarios
        query = """
            SELECT DATE(FECHA_LECTURA) as fecha, COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(FECHA_LECTURA) >= %s AND DATE(FECHA_LECTURA) <= %s
        """
        params = [start_of_week, end_of_week]
        
        if ubicacion_id:
            query += " AND ID_UBICACION = %s"
            params.append(ubicacion_id)
        
        if sensor_id:
            query += " AND ID_SENSOR = %s"
            params.append(sensor_id)
        
        query += " GROUP BY DATE(FECHA_LECTURA)"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # Crear un diccionario con todas las fechas de la semana
        dias_semana = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
        datos_diarios = []
        
        for i in range(7):
            fecha = start_of_week + timedelta(days=i)
            # Convertir fecha a formato ISO para JSON
            fecha_str = fecha.isoformat()
            
            # Buscar si hay datos para esta fecha
            total = 0
            for r in results:
                if r['fecha'] == fecha:
                    total = r['total']
                    break
            
            datos_diarios.append({
                "dia": dias_semana[i],
                "fecha": fecha_str,
                "total": total
            })
        
        # Retornar respuesta formateada
        return {
            "periodo": "semana actual",
            "datos": datos_diarios
        }
        
    finally:
        cursor.close()
