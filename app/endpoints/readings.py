from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Dict, Any
from datetime import datetime, date, time, timedelta
import mysql.connector
from app.database import get_db
from pydantic import BaseModel
import json
from hashlib import md5  # Importación para usar md5

router = APIRouter()

# Función auxiliar para calcular diferencia en días
def calcular_diferencia_dias(fecha_inicio, fecha_fin):
    """Calcula la diferencia en días entre dos fechas"""
    if not fecha_inicio or not fecha_fin:
        return 0
    try:
        inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
        fin = datetime.strptime(fecha_fin, '%Y-%m-%d').date()
        return (fin - inicio).days + 1
    except:
        return 0

# Función auxiliar para generar etiquetas por hora
def generar_etiquetas_horas():
    """Genera etiquetas para agrupación por hora"""
    return [f"{h:02d}:00 - {h:02d}:59" for h in range(24)]

# Función auxiliar para generar etiquetas por día
def generar_etiquetas_dias(fecha_inicio, fecha_fin):
    """Genera etiquetas para agrupación por día"""
    try:
        inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
        fin = datetime.strptime(fecha_fin, '%Y-%m-%d').date()
        etiquetas = []
        fecha_actual = inicio
        while fecha_actual <= fin:
            etiquetas.append(fecha_actual.strftime('%d-%m'))
            fecha_actual += timedelta(days=1)
        return etiquetas
    except:
        return []

# Función auxiliar para generar etiquetas por semana
def generar_etiquetas_semanas(fecha_inicio, fecha_fin):
    """Genera etiquetas para agrupación por semana"""
    try:
        inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
        fin = datetime.strptime(fecha_fin, '%Y-%m-%d').date()
        etiquetas = []

        # Encontrar el lunes de la primera semana
        fecha_actual = inicio - timedelta(days=inicio.weekday())

        while fecha_actual <= fin:
            # Calcular el domingo de la semana
            fin_semana = fecha_actual + timedelta(days=6)

            # Obtener número de semana ISO
            semana_iso = fecha_actual.isocalendar()[1]

            # Formatear rango de fechas
            rango = f"{fecha_actual.strftime('%d-%m')} al {fin_semana.strftime('%d-%m')}"
            etiqueta = f"Semana {semana_iso} ({rango})"

            etiquetas.append(etiqueta)
            fecha_actual += timedelta(days=7)

        return etiquetas
    except:
        return []

# Función auxiliar para generar etiquetas por mes
def generar_etiquetas_meses(fecha_inicio, fecha_fin):
    """Genera etiquetas para agrupación por mes"""
    try:
        inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
        fin = datetime.strptime(fecha_fin, '%Y-%m-%d').date()
        meses_nombres = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                        'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']

        etiquetas = []
        fecha_actual = inicio.replace(day=1)  # Primer día del mes

        while fecha_actual <= fin:
            etiquetas.append(meses_nombres[fecha_actual.month - 1])
            # Ir al siguiente mes
            if fecha_actual.month == 12:
                fecha_actual = fecha_actual.replace(year=fecha_actual.year + 1, month=1)
            else:
                fecha_actual = fecha_actual.replace(month=fecha_actual.month + 1)

        return etiquetas
    except:
        return []

# Modelos de respuesta
class LecturaBase(BaseModel):
    fecha: str
    hora: str
    comuna: str
    ubicacion: str
    sentido: Optional[str] = None  # Modificado para aceptar None
    cantidad: int

class LecturaResponse(BaseModel):
    lecturas: List[LecturaBase]
    total: int

class LecturaAgrupada(BaseModel):
    etiqueta: str
    valor: int

class ResumenResponse(BaseModel):
    etiquetas: List[str]
    datos: List[int]
    total: int

class ComunaResponse(BaseModel):
    id: int
    nombre: str

class UbicacionResponse(BaseModel):
    id: int
    nombre: str
    comuna_id: int

class SentidoResponse(BaseModel):
    id: int
    direccion: Optional[str] = None
    sentido_lectura: Optional[str] = None

# Modelos para gráfico detallado
class SerieDatos(BaseModel):
    nombre: str
    datos: List[int]
    color: Optional[str] = None

class GraficoDetalladoResponse(BaseModel):
    etiquetas: List[str]
    series: List[SerieDatos]
    total: int

# Función auxiliar para calcular semana ISO
def obtener_semana_iso(fecha):
    """Obtiene el número de semana ISO para una fecha dada"""
    return fecha.isocalendar()[1]

# Función auxiliar para obtener todas las semanas del mes actual
def obtener_semanas_mes_actual():
    """Obtiene todas las semanas ISO que tocan el mes actual"""
    hoy = datetime.now().date()
    primer_dia_mes = hoy.replace(day=1)

    # Último día del mes
    if hoy.month == 12:
        ultimo_dia_mes = date(hoy.year + 1, 1, 1) - timedelta(days=1)
    else:
        ultimo_dia_mes = date(hoy.year, hoy.month + 1, 1) - timedelta(days=1)

    # Obtener semanas que tocan el mes
    semanas = set()
    fecha_actual = primer_dia_mes
    while fecha_actual <= ultimo_dia_mes:
        semanas.add(obtener_semana_iso(fecha_actual))
        fecha_actual += timedelta(days=1)

    return sorted(list(semanas)), primer_dia_mes, ultimo_dia_mes

# Endpoint para obtener comunas
@router.get("/comunas", response_model=List[ComunaResponse])
def obtener_comunas(
    db: mysql.connector.connection.MySQLConnection = Depends(get_db)
):
    """Obtiene la lista de comunas disponibles."""
    try:
        cursor = db.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT DISTINCT
                comuna as nombre,
                MD5(comuna) as id
            FROM UBICACIONES
            ORDER BY comuna
            """
        )
        comunas = cursor.fetchall()
        cursor.close()

        # Formateamos la respuesta
        resultado = []
        for comuna in comunas:
            resultado.append({
                "id": int(comuna['id'][:8], 16) % 10000, # Convertir hash a número
                "nombre": comuna['nombre']
            })

        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener comunas: {str(e)}")

# Endpoint para obtener ubicaciones
@router.get("/ubicaciones", response_model=List[UbicacionResponse])
def obtener_ubicaciones(
    comuna_id: Optional[int] = None,
    db: mysql.connector.connection.MySQLConnection = Depends(get_db)
):
    """Obtiene la lista de ubicaciones disponibles, opcionalmente filtradas por comuna."""
    try:
        cursor = db.cursor(dictionary=True)

        query = """
            SELECT
                id_ubicacion as id,
                nombre_formal as nombre,
                comuna,
                MD5(comuna) as comuna_hash
            FROM UBICACIONES
            """

        params = []
        if comuna_id is not None:
            # Obtenemos el nombre de la comuna desde el ID
            cursor.execute(
                """
                SELECT DISTINCT
                    comuna as nombre
                FROM UBICACIONES
                ORDER BY comuna
                """
            )
            comunas = cursor.fetchall()
            comuna_map = {}
            for i, comuna in enumerate(comunas):
                hash_id = int(md5(comuna['nombre'].encode()).hexdigest()[:8], 16) % 10000
                comuna_map[hash_id] = comuna['nombre']

            if comuna_id in comuna_map:
                query += " WHERE comuna = %s"
                params.append(comuna_map[comuna_id])

        query += " ORDER BY comuna, nombre_formal"

        cursor.execute(query, params)
        ubicaciones = cursor.fetchall()
        cursor.close()

        # Formateamos la respuesta
        resultado = []
        for ubicacion in ubicaciones:
            resultado.append({
                "id": ubicacion['id'],
                "nombre": ubicacion['nombre'],
                "comuna_id": int(ubicacion['comuna_hash'][:8], 16) % 10000
            })

        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener ubicaciones: {str(e)}")

# Endpoint para obtener sentidos
@router.get("/sentidos", response_model=List[SentidoResponse])
def obtener_sentidos(
    ubicacion_id: Optional[int] = None,
    db: mysql.connector.connection.MySQLConnection = Depends(get_db)
):
    """Obtiene los sentidos de lectura disponibles, opcionalmente filtrados por ubicación."""
    try:
        cursor = db.cursor(dictionary=True)

        query = """
            SELECT DISTINCT
                ss.id AS id,
                ss.direccion,
                ss.sentido_lectura
            FROM SENTIDOS_SENSOR ss
            JOIN SENSORES s ON ss.id_sensor = s.id_sensor
        """

        params = []
        where_clauses = [
            "ss.sentido_lectura IS NOT NULL",
            "ss.sentido_lectura <> ''"
        ]

        if ubicacion_id is not None:
            where_clauses.append("s.id_ubicacion = %s")
            params.append(ubicacion_id)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += " ORDER BY ss.sentido_lectura"

        cursor.execute(query, params)
        sentidos = cursor.fetchall()
        cursor.close()

        return sentidos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener sentidos: {str(e)}")

# Endpoint para consultar lecturas
@router.get("/consulta", response_model=LecturaResponse)
def consultar_lecturas(
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
    """
    Obtiene las lecturas de bicicletas según los criterios de filtrado especificados.
    """
    try:
        cursor = db.cursor(dictionary=True)

        # Construir la query base - MODIFICADA para usar 1 como cantidad
        query = """
            SELECT
                l.fecha_lectura,
                l.comuna,
                l.ubicacion_endpoint as ubicacion,
                l.sentido_lectura as sentido,
                1 as cantidad
            FROM LECTURAS l
            JOIN UBICACIONES u ON l.id_ubicacion = u.id_ubicacion
            WHERE 1=1
        """

        params = []

        # Aplicar filtros
        if comuna_id is not None:
            # Obtener el nombre de la comuna desde el ID
            comunas_cursor = db.cursor(dictionary=True)
            comunas_cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
            comunas = comunas_cursor.fetchall()
            comunas_cursor.close()

            if 0 <= comuna_id < len(comunas):
                query += " AND l.comuna = %s"
                params.append(comunas[comuna_id]['comuna'])

        if ubicacion_id is not None:
            query += " AND l.id_ubicacion = %s"
            params.append(ubicacion_id)

        if sentidos:
            sentidos_list = [int(s.strip()) for s in sentidos.split(',') if s.strip().isdigit()]
            if sentidos_list:
                # Obtener los valores de sentido_lectura correspondientes a los IDs
                sentidos_placeholders = ", ".join(["%s"] * len(sentidos_list))
                sentidos_query = f"""
                    SELECT sentido_lectura
                    FROM SENTIDOS_SENSOR
                    WHERE id IN ({sentidos_placeholders})
                """
                sentidos_cursor = db.cursor(dictionary=True)
                sentidos_cursor.execute(sentidos_query, sentidos_list)
                sentidos_valores = [row['sentido_lectura'] for row in sentidos_cursor.fetchall()]
                sentidos_cursor.close()

                if sentidos_valores:
                    sentidos_valores_placeholders = ", ".join(["%s"] * len(sentidos_valores))
                    query += f" AND l.sentido_lectura IN ({sentidos_valores_placeholders})"
                    params.extend(sentidos_valores)

        # Filtros de tiempo según período
        hoy = datetime.now().date()

        if periodo == "hoy":
            query += " AND DATE(l.fecha_lectura) = %s"
            params.append(hoy.strftime('%Y-%m-%d'))

        elif periodo == "semana":
            # Primer día de la semana (lunes)
            dia_semana = hoy.weekday()
            inicio_semana = hoy - timedelta(days=dia_semana)
            fin_semana = inicio_semana + timedelta(days=6)

            query += " AND DATE(l.fecha_lectura) >= %s AND DATE(l.fecha_lectura) <= %s"
            params.append(inicio_semana.strftime('%Y-%m-%d'))
            params.append(fin_semana.strftime('%Y-%m-%d'))

        elif periodo == "mes":
            # Primer y último día del mes actual
            primer_dia_mes = date(hoy.year, hoy.month, 1)
            if hoy.month == 12:
                ultimo_dia_mes = date(hoy.year + 1, 1, 1) - timedelta(days=1)
            else:
                ultimo_dia_mes = date(hoy.year, hoy.month + 1, 1) - timedelta(days=1)

            query += " AND DATE(l.fecha_lectura) >= %s AND DATE(l.fecha_lectura) <= %s"
            params.append(primer_dia_mes.strftime('%Y-%m-%d'))
            params.append(ultimo_dia_mes.strftime('%Y-%m-%d'))

        elif periodo == "anio":
            # Primer y último día del año actual
            primer_dia_anio = date(hoy.year, 1, 1)
            ultimo_dia_anio = date(hoy.year, 12, 31)

            query += " AND DATE(l.fecha_lectura) >= %s AND DATE(l.fecha_lectura) <= %s"
            params.append(primer_dia_anio.strftime('%Y-%m-%d'))
            params.append(ultimo_dia_anio.strftime('%Y-%m-%d'))

        elif periodo == "personalizado":
            if fecha_inicio:
                query += " AND DATE(l.fecha_lectura) >= %s"
                params.append(fecha_inicio)

            if fecha_fin:
                query += " AND DATE(l.fecha_lectura) <= %s"
                params.append(fecha_fin)

            # Filtros de hora
            if hora_inicio:
                query += " AND TIME(l.fecha_lectura) >= %s"
                params.append(hora_inicio)

            if hora_fin:
                query += " AND TIME(l.fecha_lectura) <= %s"
                params.append(hora_fin)

        # Ordenar por fecha
        query += " ORDER BY l.fecha_lectura DESC"

        # Limitar a 1000 resultados para evitar problemas de rendimiento
        query += " LIMIT 1000"

        cursor.execute(query, params)
        results = cursor.fetchall()

        # Formatear los resultados
        lecturas = []
        for row in results:
            fecha_hora = row['fecha_lectura']
            lecturas.append({
                "fecha": fecha_hora.strftime('%d/%m/%Y'),
                "hora": fecha_hora.strftime('%H:%M'),
                "comuna": row['comuna'],
                "ubicacion": row['ubicacion'],
                "sentido": row['sentido'],  # Ahora puede ser None
                "cantidad": row['cantidad']  # Ahora es 1 para cada registro
            })

        # Obtener el total - MODIFICADO para contar registros
        total = len(results)

        cursor.close()
        return {"lecturas": lecturas, "total": total}

    except Exception as e:
        import traceback
        print(f"Error al consultar lecturas: {str(e)}")
        print(traceback.format_exc())

        # Si ocurre un error, devolvemos un resultado vacío
        return {"lecturas": [], "total": 0}

@router.get("/grafico", response_model=ResumenResponse)
def obtener_datos_grafico(
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
    """
    Obtiene datos agrupados para mostrar en gráficos, asegurando que no se duplique el conteo por sentidos.
    Agrupa por hora, día, semana o mes según el período y filtros proporcionados.
    """
    try:
        cursor = db.cursor(dictionary=True)

        # NUEVA LÓGICA ESPECÍFICA PARA PERÍODO MES
        if periodo == "mes":
            # Obtener información del mes actual
            semanas_mes, primer_dia_mes, ultimo_dia_mes = obtener_semanas_mes_actual()

            # Consulta SQL para agrupar por semana ISO
            query = """
                SELECT
                    WEEK(fecha_lectura, 1) AS semana_iso,
                    COUNT(*) AS total_lecturas
                FROM LECTURAS
                WHERE DATE(fecha_lectura) BETWEEN %s AND %s
            """

            params = [primer_dia_mes.strftime('%Y-%m-%d'), ultimo_dia_mes.strftime('%Y-%m-%d')]

            # Aplicar filtros adicionales si existen
            if comuna_id is not None:
                cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
                comunas = cursor.fetchall()
                if comunas and 0 <= comuna_id < len(comunas):
                    query += " AND comuna = %s"
                    params.append(comunas[comuna_id]['comuna'])

            if ubicacion_id is not None:
                query += " AND id_ubicacion = %s"
                params.append(ubicacion_id)

            if sentidos:
                sentidos_list = [int(s.strip()) for s in sentidos.split(',') if s.strip().isdigit()]
                if sentidos_list:
                    # Obtener los valores de sentido_lectura correspondientes a los IDs
                    sentidos_placeholders = ", ".join(["%s"] * len(sentidos_list))
                    sentidos_query = f"""
                        SELECT sentido_lectura
                        FROM SENTIDOS_SENSOR
                        WHERE id IN ({sentidos_placeholders})
                    """
                    sentidos_cursor = db.cursor(dictionary=True)
                    sentidos_cursor.execute(sentidos_query, sentidos_list)
                    sentidos_valores = [row['sentido_lectura'] for row in sentidos_cursor.fetchall()]
                    sentidos_cursor.close()

                    if sentidos_valores:
                        sentidos_valores_placeholders = ", ".join(["%s"] * len(sentidos_valores))
                        query += f" AND sentido_lectura IN ({sentidos_valores_placeholders})"
                        params.extend(sentidos_valores)

            # Finalizar consulta
            query += " GROUP BY WEEK(fecha_lectura, 1) ORDER BY WEEK(fecha_lectura, 1)"

            cursor.execute(query, params)
            resultados = cursor.fetchall()

            # Preparar datos en formato adecuado
            datos_por_semana = {}
            for row in resultados:
                semana = row['semana_iso']
                cantidad = row['total_lecturas']
                datos_por_semana[semana] = cantidad

            # Crear etiquetas y datos para todas las semanas del mes
            etiquetas = []
            datos = []
            total = 0

            for semana in semanas_mes:
                etiquetas.append(f"Semana {semana}")
                cantidad = datos_por_semana.get(semana, 0)
                datos.append(cantidad)
                total += cantidad

            cursor.close()
            return {
                "etiquetas": etiquetas,
                "datos": datos,
                "total": total
            }

        # NUEVA LÓGICA ESPECÍFICA PARA PERÍODO AÑO
        if periodo == "anio":
            # Obtener el año actual
            hoy = datetime.now().date()
            año_actual = hoy.year

            # Consulta SQL para agrupar por mes
            query = """
                SELECT
                    MONTH(fecha_lectura) AS mes,
                    COUNT(*) AS total_lecturas
                FROM LECTURAS
                WHERE YEAR(fecha_lectura) = %s
            """

            params = [año_actual]

            # Aplicar filtros adicionales si existen
            if comuna_id is not None:
                cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
                comunas = cursor.fetchall()
                if comunas and 0 <= comuna_id < len(comunas):
                    query += " AND comuna = %s"
                    params.append(comunas[comuna_id]['comuna'])

            if ubicacion_id is not None:
                query += " AND id_ubicacion = %s"
                params.append(ubicacion_id)

            if sentidos:
                sentidos_list = [int(s.strip()) for s in sentidos.split(',') if s.strip().isdigit()]
                if sentidos_list:
                    # Obtener los valores de sentido_lectura correspondientes a los IDs
                    sentidos_placeholders = ", ".join(["%s"] * len(sentidos_list))
                    sentidos_query = f"""
                        SELECT sentido_lectura
                        FROM SENTIDOS_SENSOR
                        WHERE id IN ({sentidos_placeholders})
                    """
                    sentidos_cursor = db.cursor(dictionary=True)
                    sentidos_cursor.execute(sentidos_query, sentidos_list)
                    sentidos_valores = [row['sentido_lectura'] for row in sentidos_cursor.fetchall()]
                    sentidos_cursor.close()

                    if sentidos_valores:
                        sentidos_valores_placeholders = ", ".join(["%s"] * len(sentidos_valores))
                        query += f" AND sentido_lectura IN ({sentidos_valores_placeholders})"
                        params.extend(sentidos_valores)

            # Finalizar consulta
            query += " GROUP BY MONTH(fecha_lectura) ORDER BY MONTH(fecha_lectura)"

            cursor.execute(query, params)
            resultados = cursor.fetchall()

            # Preparar datos en formato adecuado
            meses_nombres = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                           'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']

            # Inicializar datos con ceros para todos los 12 meses
            datos_por_mes = [0] * 12

            # Llenar con los datos reales
            for row in resultados:
                mes_num = row['mes']  # 1-12 desde MySQL
                if 1 <= mes_num <= 12:
                    datos_por_mes[mes_num - 1] = row['total_lecturas']  # Convertir a índice 0-11

            # Mostrar todos los 12 meses del año
            etiquetas = meses_nombres  # Todos los 12 meses
            datos = datos_por_mes      # Todos los 12 meses (con 0 si no hay datos)
            total = sum(datos)

            cursor.close()
            return {
                "etiquetas": etiquetas,
                "datos": datos,
                "total": total
            }

        # Manejo específico para período SEMANA (código original)
        if periodo == "semana":
            # Primer día de la semana (lunes)
            hoy = datetime.now().date()
            dia_semana = hoy.weekday()
            inicio_semana = hoy - timedelta(days=dia_semana)
            fin_semana = inicio_semana + timedelta(days=6)

            # Calcular número de semana ISO
            semana_iso = inicio_semana.isocalendar()[1]

            # Formatear el rango de fechas para el título del gráfico
            rango_fechas = f"{inicio_semana.strftime('%d-%m')} al {fin_semana.strftime('%d-%m')}"

            # Consulta SQL para agrupar por día de la semana
            query = """
                SELECT
                    DATE_FORMAT(fecha_lectura, '%w') AS dia_semana_num,
                    DAYNAME(fecha_lectura) AS nombre_dia,
                    COUNT(*) AS total_lecturas
                FROM LECTURAS
                WHERE DATE(fecha_lectura) BETWEEN %s AND %s
            """

            params = [inicio_semana.strftime('%Y-%m-%d'), fin_semana.strftime('%Y-%m-%d')]

            # Aplicar filtros adicionales si existen
            if comuna_id is not None:
                cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
                comunas = cursor.fetchall()
                if comunas and 0 <= comuna_id < len(comunas):
                    query += " AND comuna = %s"
                    params.append(comunas[comuna_id]['comuna'])

            if ubicacion_id is not None:
                query += " AND id_ubicacion = %s"
                params.append(ubicacion_id)

            if sentidos:
                sentidos_list = [int(s.strip()) for s in sentidos.split(',') if s.strip().isdigit()]
                if sentidos_list:
                    # Obtener los valores de sentido_lectura correspondientes a los IDs
                    sentidos_placeholders = ", ".join(["%s"] * len(sentidos_list))
                    sentidos_query = f"""
                        SELECT sentido_lectura
                        FROM SENTIDOS_SENSOR
                        WHERE id IN ({sentidos_placeholders})
                    """
                    sentidos_cursor = db.cursor(dictionary=True)
                    sentidos_cursor.execute(sentidos_query, sentidos_list)
                    sentidos_valores = [row['sentido_lectura'] for row in sentidos_cursor.fetchall()]
                    sentidos_cursor.close()

                    if sentidos_valores:
                        sentidos_valores_placeholders = ", ".join(["%s"] * len(sentidos_valores))
                        query += f" AND sentido_lectura IN ({sentidos_valores_placeholders})"
                        params.extend(sentidos_valores)

            # Finalizar consulta
            query += """
                GROUP BY DATE_FORMAT(fecha_lectura, '%w'), DAYNAME(fecha_lectura)
                ORDER BY DATE_FORMAT(fecha_lectura, '%w')
            """

            cursor.execute(query, params)
            resultados = cursor.fetchall()

            # Preparar datos en formato adecuado
            dias_semana = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
            datos = [0] * 7  # Un espacio para cada día de la semana
            etiquetas = dias_semana.copy()

            # Mapear resultados a los días correspondientes
            for row in resultados:
                # MySQL DATE_FORMAT('%w') devuelve 0=Dom, 1=Lun, ..., 6=Sáb
                # Ajustamos al formato Lun, Mar, ..., Dom
                dia_num = int(row['dia_semana_num'])
                indice = dia_num if dia_num > 0 else 6  # Convertir 0 (domingo) a 6
                indice = indice - 1  # Ajustar para nuestro array (0=Lun, 1=Mar, etc.)
                datos[indice] = row['total_lecturas']

            # Incluir el número de semana ISO y rango de fechas en la primera etiqueta
            etiquetas[0] = f"Semana {semana_iso} ({rango_fechas}) - {etiquetas[0]}"

            cursor.close()
            return {
                "etiquetas": etiquetas,
                "datos": datos,
                "total": sum(datos),
                "series": [  # Agregar las series necesarias
                    {
                        "nombre": "Total",
                        "datos": datos,
                        "color": "#6366f1"
                    }
                ]
            }

        # NUEVA LÓGICA ESPECÍFICA PARA PERÍODO PERSONALIZADO CON AGRUPACIÓN FLEXIBLE
        elif periodo == "personalizado":
            # Obtener parámetro de agrupación (por defecto 'dia')
            agrupar_por_param = agrupar_por if agrupar_por else 'dia'

            # Validar fechas
            if not fecha_inicio or not fecha_fin:
                cursor.close()
                return {
                    "etiquetas": [],
                    "datos": [],
                    "total": 0
                }

            # Calcular diferencia de días para validaciones
            dias_diferencia = calcular_diferencia_dias(fecha_inicio, fecha_fin)

            # Validar agrupación por hora (solo para rangos ≤ 7 días)
            if agrupar_por_param == 'hora' and dias_diferencia > 7:
                agrupar_por_param = 'dia'  # Cambiar a día si el rango es muy grande

            # Construir consulta base
            query_base = """
                SELECT
                    fecha_lectura,
                    COUNT(*) AS total_lecturas
                FROM LECTURAS
                WHERE DATE(fecha_lectura) >= %s AND DATE(fecha_lectura) <= %s
            """
            params = [fecha_inicio, fecha_fin]

            # Añadir filtros de hora si existen
            if hora_inicio:
                query_base += " AND TIME(fecha_lectura) >= %s"
                params.append(hora_inicio)
            if hora_fin:
                query_base += " AND TIME(fecha_lectura) <= %s"
                params.append(hora_fin)

            # Aplicar filtros adicionales
            if comuna_id is not None:
                cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
                comunas = cursor.fetchall()
                if comunas and 0 <= comuna_id < len(comunas):
                    query_base += " AND comuna = %s"
                    params.append(comunas[comuna_id]['comuna'])

            if ubicacion_id is not None:
                query_base += " AND id_ubicacion = %s"
                params.append(ubicacion_id)

            if sentidos:
                sentidos_list = [int(s.strip()) for s in sentidos.split(',') if s.strip().isdigit()]
                if sentidos_list:
                    sentidos_placeholders = ", ".join(["%s"] * len(sentidos_list))
                    sentidos_query = f"""
                        SELECT sentido_lectura
                        FROM SENTIDOS_SENSOR
                        WHERE id IN ({sentidos_placeholders})
                    """
                    sentidos_cursor = db.cursor(dictionary=True)
                    sentidos_cursor.execute(sentidos_query, sentidos_list)
                    sentidos_valores = [row['sentido_lectura'] for row in sentidos_cursor.fetchall()]
                    sentidos_cursor.close()

                    if sentidos_valores:
                        sentidos_valores_placeholders = ", ".join(["%s"] * len(sentidos_valores))
                        query_base += f" AND sentido_lectura IN ({sentidos_valores_placeholders})"
                        params.extend(sentidos_valores)

            # Aplicar agrupación según el parámetro
            if agrupar_por_param == 'hora':
                query = query_base + " GROUP BY HOUR(fecha_lectura) ORDER BY HOUR(fecha_lectura)"
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                etiquetas = generar_etiquetas_horas()
                datos = [0] * 24

                for row in resultados:
                    hora = datetime.fromisoformat(str(row['fecha_lectura'])).hour
                    datos[hora] = row['total_lecturas']

            elif agrupar_por_param == 'dia':
                query = query_base + " GROUP BY DATE(fecha_lectura) ORDER BY DATE(fecha_lectura)"
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                etiquetas = generar_etiquetas_dias(fecha_inicio, fecha_fin)
                datos = [0] * len(etiquetas)

                # Crear mapa de fechas para indexación rápida
                fecha_to_index = {}
                fecha_actual = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
                for i, _ in enumerate(etiquetas):
                    fecha_to_index[fecha_actual.strftime('%d-%m')] = i
                    fecha_actual += timedelta(days=1)

                for row in resultados:
                    fecha = datetime.fromisoformat(str(row['fecha_lectura'])).date()
                    fecha_str = fecha.strftime('%d-%m')
                    if fecha_str in fecha_to_index:
                        datos[fecha_to_index[fecha_str]] = row['total_lecturas']

            elif agrupar_por_param == 'semana':
                query = query_base + " GROUP BY YEARWEEK(fecha_lectura, 1) ORDER BY YEARWEEK(fecha_lectura, 1)"
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                etiquetas = generar_etiquetas_semanas(fecha_inicio, fecha_fin)
                datos = [0] * len(etiquetas)

                # Mapear resultados por semana
                for i, row in enumerate(resultados):
                    if i < len(datos):
                        datos[i] = row['total_lecturas']

            elif agrupar_por_param == 'mes':
                query = query_base + " GROUP BY YEAR(fecha_lectura), MONTH(fecha_lectura) ORDER BY YEAR(fecha_lectura), MONTH(fecha_lectura)"
                cursor.execute(query, params)
                resultados = cursor.fetchall()

                etiquetas = generar_etiquetas_meses(fecha_inicio, fecha_fin)
                datos = [0] * len(etiquetas)

                # Mapear resultados por mes
                for i, row in enumerate(resultados):
                    if i < len(datos):
                        datos[i] = row['total_lecturas']

            else:
                # Por defecto usar agrupación por día
                etiquetas = generar_etiquetas_dias(fecha_inicio, fecha_fin)
                datos = [0] * len(etiquetas)

            total = sum(datos)

            cursor.close()
            return {
                "etiquetas": etiquetas,
                "datos": datos,
                "total": total
            }

        # Procesamiento original para período HOY
        etiquetas = [f"{h:02d}:00 - {h:02d}:59" for h in range(24)]  # etiquetas fijas por hora
        datos = [0] * 24
        total = 0

        filtros = ["DATE(fecha_lectura) = CURDATE()"]
        params = []

        if comuna_id is not None:
            cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
            comunas = cursor.fetchall()
            if comunas and 0 <= comuna_id < len(comunas):
                filtros.append("comuna = %s")
                params.append(comunas[comuna_id]['comuna'])

        if ubicacion_id is not None:
            filtros.append("id_ubicacion = %s")
            params.append(ubicacion_id)

        if sentidos:
            sentidos_ids = [int(s) for s in sentidos.split(',') if s.strip().isdigit()]
            if sentidos_ids:
                placeholders = ', '.join(['%s'] * len(sentidos_ids))
                cursor.execute(f"SELECT sentido_lectura FROM SENTIDOS_SENSOR WHERE id IN ({placeholders})", sentidos_ids)
                sentidos_texto = [row['sentido_lectura'] for row in cursor.fetchall()]
                if sentidos_texto:
                    placeholders_texto = ', '.join(['%s'] * len(sentidos_texto))
                    filtros.append(f"sentido_lectura IN ({placeholders_texto})")
                    params.extend(sentidos_texto)

        where_clause = " AND ".join(filtros)

        query = f"""
            SELECT
                HOUR(fecha_lectura) AS hora,
                COUNT(*) AS total
            FROM LECTURAS
            WHERE {where_clause}
            GROUP BY HOUR(fecha_lectura)
            ORDER BY HOUR(fecha_lectura)
        """

        cursor.execute(query, params)
        resultados = cursor.fetchall()

        for row in resultados:
            hora = row['hora']
            cantidad = row['total']
            datos[hora] = cantidad
            total += cantidad

        cursor.close()

        return {
            "etiquetas": etiquetas,
            "datos": datos,
            "total": total
        }

    except Exception as e:
        import traceback
        print(f"Error al obtener datos para gráfico: {str(e)}")
        print(traceback.format_exc())
        return {
            "etiquetas": [],
            "datos": [],
            "total": 0
        }

# Endpoint para obtener resumen de lecturas
@router.get("/resumen")
def obtener_resumen(
    db: mysql.connector.connection.MySQLConnection = Depends(get_db)
):
    """
    Obtiene un resumen estadístico de las lecturas (total hoy, total mes, variación, etc.)
    """
    try:
        cursor = db.cursor(dictionary=True)

        # Fecha actual
        hoy = datetime.now().date()
        ayer = hoy - timedelta(days=1)

        # Primer y último día del mes actual
        primer_dia_mes = date(hoy.year, hoy.month, 1)
        if hoy.month == 12:
            ultimo_dia_mes = date(hoy.year + 1, 1, 1) - timedelta(days=1)
        else:
            ultimo_dia_mes = date(hoy.year, hoy.month + 1, 1) - timedelta(days=1)

        # Primer y último día del mes anterior
        if hoy.month == 1:
            primer_dia_mes_anterior = date(hoy.year - 1, 12, 1)
            ultimo_dia_mes_anterior = date(hoy.year, 1, 1) - timedelta(days=1)
        else:
            primer_dia_mes_anterior = date(hoy.year, hoy.month - 1, 1)
            ultimo_dia_mes_anterior = primer_dia_mes - timedelta(days=1)

        # Total hoy - MODIFICADO para usar COUNT(*)
        cursor.execute(
            "SELECT COUNT(*) as total FROM LECTURAS WHERE DATE(fecha_lectura) = %s",
            (hoy.strftime('%Y-%m-%d'),)
        )
        total_hoy = cursor.fetchone()['total'] or 0

        # Total ayer - MODIFICADO para usar COUNT(*)
        cursor.execute(
            "SELECT COUNT(*) as total FROM LECTURAS WHERE DATE(fecha_lectura) = %s",
            (ayer.strftime('%Y-%m-%d'),)
        )
        total_ayer = cursor.fetchone()['total'] or 0

        # Variación porcentual diaria
        variacion_diaria = 0
        if total_ayer > 0:
            variacion_diaria = ((total_hoy - total_ayer) / total_ayer) * 100

        # Total mes actual - MODIFICADO para usar COUNT(*)
        cursor.execute(
            """
            SELECT COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(fecha_lectura) >= %s AND DATE(fecha_lectura) <= %s
            """,
            (primer_dia_mes.strftime('%Y-%m-%d'), ultimo_dia_mes.strftime('%Y-%m-%d'))
        )
        total_mes = cursor.fetchone()['total'] or 0

        # Total mes anterior - MODIFICADO para usar COUNT(*)
        cursor.execute(
            """
            SELECT COUNT(*) as total
            FROM LECTURAS
            WHERE DATE(fecha_lectura) >= %s AND DATE(fecha_lectura) <= %s
            """,
            (primer_dia_mes_anterior.strftime('%Y-%m-%d'), ultimo_dia_mes_anterior.strftime('%Y-%m-%d'))
        )
        total_mes_anterior = cursor.fetchone()['total'] or 0

        # Variación porcentual mensual
        variacion_mensual = 0
        if total_mes_anterior > 0:
            variacion_mensual = ((total_mes - total_mes_anterior) / total_mes_anterior) * 100

        # Promedio diario último mes
        dias_mes = (ultimo_dia_mes - primer_dia_mes).days + 1
        promedio_diario = total_mes / dias_mes if dias_mes > 0 else 0

        # Total sensores
        cursor.execute("SELECT COUNT(*) as total FROM SENSORES")
        total_sensores = cursor.fetchone()['total']

        # Sensores activos/inactivos
        cursor.execute("SELECT COUNT(*) as total FROM SENSORES WHERE estado_sensor = 'active'")
        sensores_activos = cursor.fetchone()['total']

        sensores_inactivos = total_sensores - sensores_activos

        cursor.close()

        return {
            "ciclistas_hoy": {
                "total": total_hoy,
                "variacion_porcentual": round(variacion_diaria, 2)
            },
            "ciclistas_mes": {
                "total": total_mes,
                "variacion_porcentual": round(variacion_mensual, 2)
            },
            "promedio_diario": {
                "total": round(promedio_diario, 2),
                "variacion_porcentual": 0  # Calcular si es necesario
            },
            "sensores": {
                "total": total_sensores,
                "activos": sensores_activos,
                "inactivos": sensores_inactivos
            }
        }

    except Exception as e:
        import traceback
        print(f"Error al obtener resumen: {str(e)}")
        print(traceback.format_exc())
        # En caso de error, retornar datos básicos
        return {
            "ciclistas_hoy": {"total": 0, "variacion_porcentual": 0},
            "ciclistas_mes": {"total": 0, "variacion_porcentual": 0},
            "promedio_diario": {"total": 0, "variacion_porcentual": 0},
            "sensores": {"total": 0, "activos": 0, "inactivos": 0}
        }

@router.get("/summary")
def resumen_alias(db: mysql.connector.connection.MySQLConnection = Depends(get_db)):
    return obtener_resumen(db)



@router.get("/grafico_detallado", response_model=GraficoDetalladoResponse)
def obtener_datos_grafico_detallado(
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
    """
    Devuelve los datos detallados por sentido de lectura y la serie Total sumada por franja horaria.
    Aplica para cualquier período.
    """
    try:
        cursor = db.cursor(dictionary=True)

        # NUEVA LÓGICA ESPECÍFICA PARA PERÍODO MES EN GRÁFICO DETALLADO
        if periodo == "mes":
            # Obtener información del mes actual
            semanas_mes, primer_dia_mes, ultimo_dia_mes = obtener_semanas_mes_actual()

            # Consulta SQL mejorada para agrupar por semana ISO y sentido
            query = """
                SELECT
                    WEEK(fecha_lectura, 1) AS semana_iso,
                    sentido_lectura,
                    COUNT(*) AS total_lecturas
                FROM LECTURAS
                WHERE DATE(fecha_lectura) BETWEEN %s AND %s
            """

            params = [primer_dia_mes.strftime('%Y-%m-%d'), ultimo_dia_mes.strftime('%Y-%m-%d')]

            # Aplicar filtros adicionales si existen
            if comuna_id is not None:
                cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
                comunas = cursor.fetchall()
                if comunas and 0 <= comuna_id < len(comunas):
                    query += " AND comuna = %s"
                    params.append(comunas[comuna_id]['comuna'])

            if ubicacion_id is not None:
                query += " AND id_ubicacion = %s"
                params.append(ubicacion_id)

            if sentidos:
                sentidos_list = [int(s.strip()) for s in sentidos.split(',') if s.strip().isdigit()]
                if sentidos_list:
                    # Obtener los valores de sentido_lectura correspondientes a los IDs
                    sentidos_placeholders = ", ".join(["%s"] * len(sentidos_list))
                    sentidos_query = f"""
                        SELECT sentido_lectura
                        FROM SENTIDOS_SENSOR
                        WHERE id IN ({sentidos_placeholders})
                    """
                    sentidos_cursor = db.cursor(dictionary=True)
                    sentidos_cursor.execute(sentidos_query, sentidos_list)
                    sentidos_valores = [row['sentido_lectura'] for row in sentidos_cursor.fetchall()]
                    sentidos_cursor.close()

                    if sentidos_valores:
                        sentidos_valores_placeholders = ", ".join(["%s"] * len(sentidos_valores))
                        query += f" AND sentido_lectura IN ({sentidos_valores_placeholders})"
                        params.extend(sentidos_valores)

            # Finalizar consulta
            query += """
                GROUP BY WEEK(fecha_lectura, 1), sentido_lectura
                ORDER BY WEEK(fecha_lectura, 1), sentido_lectura
            """

            cursor.execute(query, params)
            resultados = cursor.fetchall()

            # Preparar datos en formato adecuado
            datos_por_sentido = {}

            for row in resultados:
                semana = row['semana_iso']
                sentido = row['sentido_lectura'] or 'Sin sentido'
                cantidad = row['total_lecturas']

                # Si no existe el sentido, inicializarlo con ceros para todas las semanas
                if sentido not in datos_por_sentido:
                    datos_por_sentido[sentido] = {}

                # Asignar la cantidad a la semana correspondiente
                datos_por_sentido[sentido][semana] = cantidad

            # Preparar las series para la respuesta
            series = []
            colores = [
                "#4f46e5", "#7c3aed", "#0891b2", "#15803d", "#ca8a04",
                "#dc2626", "#ea580c", "#db2777", "#8b5cf6", "#84cc16"
            ]

            # Calcular el total por semana
            total_por_semana = {}

            for idx, (sentido, datos_semanas) in enumerate(datos_por_sentido.items()):
                # Crear array de datos para todas las semanas del mes
                datos_array = []
                for semana in semanas_mes:
                    cantidad = datos_semanas.get(semana, 0)
                    datos_array.append(cantidad)

                    # Sumar al total por semana
                    if semana not in total_por_semana:
                        total_por_semana[semana] = 0
                    total_por_semana[semana] += cantidad

                series.append(SerieDatos(
                    nombre=sentido,
                    datos=datos_array,
                    color=colores[idx % len(colores)]
                ))

            # Crear array de totales ordenado
            total_array = [total_por_semana.get(semana, 0) for semana in semanas_mes]

            # Crear etiquetas
            etiquetas = [f"Semana {semana}" for semana in semanas_mes]

            # Agregar la serie de "Total" al inicio
            series.insert(0, SerieDatos(
                nombre="Total",
                datos=total_array,
                color="#6366f1"
            ))

            cursor.close()
            return GraficoDetalladoResponse(
                etiquetas=etiquetas,
                series=series,
                total=sum(total_array)
            )

        # NUEVA LÓGICA ESPECÍFICA PARA PERÍODO AÑO EN GRÁFICO DETALLADO
        if periodo == "anio":
            # Obtener el año actual
            hoy = datetime.now().date()
            año_actual = hoy.year

            # Consulta SQL mejorada para agrupar por mes y sentido
            query = """
                SELECT
                    MONTH(fecha_lectura) AS mes,
                    sentido_lectura,
                    COUNT(*) AS total_lecturas
                FROM LECTURAS
                WHERE YEAR(fecha_lectura) = %s
            """

            params = [año_actual]

            # Aplicar filtros adicionales si existen
            if comuna_id is not None:
                cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
                comunas = cursor.fetchall()
                if comunas and 0 <= comuna_id < len(comunas):
                    query += " AND comuna = %s"
                    params.append(comunas[comuna_id]['comuna'])

            if ubicacion_id is not None:
                query += " AND id_ubicacion = %s"
                params.append(ubicacion_id)

            if sentidos:
                sentidos_list = [int(s.strip()) for s in sentidos.split(',') if s.strip().isdigit()]
                if sentidos_list:
                    # Obtener los valores de sentido_lectura correspondientes a los IDs
                    sentidos_placeholders = ", ".join(["%s"] * len(sentidos_list))
                    sentidos_query = f"""
                        SELECT sentido_lectura
                        FROM SENTIDOS_SENSOR
                        WHERE id IN ({sentidos_placeholders})
                    """
                    sentidos_cursor = db.cursor(dictionary=True)
                    sentidos_cursor.execute(sentidos_query, sentidos_list)
                    sentidos_valores = [row['sentido_lectura'] for row in sentidos_cursor.fetchall()]
                    sentidos_cursor.close()

                    if sentidos_valores:
                        sentidos_valores_placeholders = ", ".join(["%s"] * len(sentidos_valores))
                        query += f" AND sentido_lectura IN ({sentidos_valores_placeholders})"
                        params.extend(sentidos_valores)

            # Finalizar consulta
            query += """
                GROUP BY MONTH(fecha_lectura), sentido_lectura
                ORDER BY MONTH(fecha_lectura), sentido_lectura
            """

            cursor.execute(query, params)
            resultados = cursor.fetchall()

            # Preparar datos en formato adecuado
            meses_nombres = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                           'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']

            # Mostrar todos los 12 meses del año
            etiquetas = meses_nombres  # Todos los 12 meses

            datos_por_sentido = {}

            for row in resultados:
                mes_num = row['mes']  # 1-12 desde MySQL
                sentido = row['sentido_lectura'] or 'Sin sentido'
                cantidad = row['total_lecturas']

                # Procesar todos los meses (1-12)
                if 1 <= mes_num <= 12:
                    # Si no existe el sentido, inicializarlo con ceros para todos los 12 meses
                    if sentido not in datos_por_sentido:
                        datos_por_sentido[sentido] = [0] * 12

                    # Asignar la cantidad al mes correspondiente (convertir 1-12 a 0-11)
                    datos_por_sentido[sentido][mes_num - 1] = cantidad

            # Preparar las series para la respuesta
            series = []
            colores = [
                "#4f46e5", "#7c3aed", "#0891b2", "#15803d", "#ca8a04",
                "#dc2626", "#ea580c", "#db2777", "#8b5cf6", "#84cc16"
            ]

            # Calcular el total por mes (todos los 12 meses)
            total_por_mes = [0] * 12

            for idx, (sentido, datos_meses) in enumerate(datos_por_sentido.items()):
                series.append(SerieDatos(
                    nombre=sentido,
                    datos=datos_meses,
                    color=colores[idx % len(colores)]
                ))

                # Sumar al total por mes
                total_por_mes = [a + b for a, b in zip(total_por_mes, datos_meses)]

            # Agregar la serie de "Total" al inicio
            series.insert(0, SerieDatos(
                nombre="Total",
                datos=total_por_mes,
                color="#6366f1"
            ))

            cursor.close()
            return GraficoDetalladoResponse(
                etiquetas=etiquetas,
                series=series,
                total=sum(total_por_mes)
            )

        # Tratamiento específico para período SEMANA (código original)
        if periodo == "semana":
            # Primer día de la semana (lunes)
            hoy = datetime.now().date()
            dia_semana = hoy.weekday()
            inicio_semana = hoy - timedelta(days=dia_semana)
            fin_semana = inicio_semana + timedelta(days=6)

            # Calcular número de semana ISO
            semana_iso = inicio_semana.isocalendar()[1]

            # Formatear el rango de fechas para el título del gráfico
            rango_fechas = f"{inicio_semana.strftime('%d-%m')} al {fin_semana.strftime('%d-%m')}"

            # Consulta SQL mejorada para agrupar por día de la semana y sentido
            query = """
                SELECT
                    DATE_FORMAT(fecha_lectura, '%w') AS dia_semana_num,
                    sentido_lectura,
                    COUNT(*) AS total_lecturas
                FROM LECTURAS
                WHERE DATE(fecha_lectura) BETWEEN %s AND %s
            """

            params = [inicio_semana.strftime('%Y-%m-%d'), fin_semana.strftime('%Y-%m-%d')]

            # Aplicar filtros adicionales si existen
            if comuna_id is not None:
                cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
                comunas = cursor.fetchall()
                if comunas and 0 <= comuna_id < len(comunas):
                    query += " AND comuna = %s"
                    params.append(comunas[comuna_id]['comuna'])

            if ubicacion_id is not None:
                query += " AND id_ubicacion = %s"
                params.append(ubicacion_id)

            if sentidos:
                sentidos_list = [int(s.strip()) for s in sentidos.split(',') if s.strip().isdigit()]
                if sentidos_list:
                    # Obtener los valores de sentido_lectura correspondientes a los IDs
                    sentidos_placeholders = ", ".join(["%s"] * len(sentidos_list))
                    sentidos_query = f"""
                        SELECT sentido_lectura
                        FROM SENTIDOS_SENSOR
                        WHERE id IN ({sentidos_placeholders})
                    """
                    sentidos_cursor = db.cursor(dictionary=True)
                    sentidos_cursor.execute(sentidos_query, sentidos_list)
                    sentidos_valores = [row['sentido_lectura'] for row in sentidos_cursor.fetchall()]
                    sentidos_cursor.close()

                    if sentidos_valores:
                        sentidos_valores_placeholders = ", ".join(["%s"] * len(sentidos_valores))
                        query += f" AND sentido_lectura IN ({sentidos_valores_placeholders})"
                        params.extend(sentidos_valores)

            # Finalizar consulta
            query += """
                GROUP BY DATE_FORMAT(fecha_lectura, '%w'), sentido_lectura
                ORDER BY DATE_FORMAT(fecha_lectura, '%w'), sentido_lectura
            """

            cursor.execute(query, params)
            resultados = cursor.fetchall()

            # Preparar datos en formato adecuado
            dias_semana = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
            datos_por_sentido = {}

            for row in resultados:
                # MySQL DATE_FORMAT('%w') devuelve 0=Dom, 1=Lun, ..., 6=Sáb
                dia_num = int(row['dia_semana_num'])
                indice = dia_num if dia_num > 0 else 6  # Convertir 0 (domingo) a 6
                indice = indice - 1  # Ajustar para nuestro array (0=Lun, 1=Mar, etc.)

                sentido = row['sentido_lectura'] or 'Sin sentido'
                cantidad = row['total_lecturas']

                # Si no existe el sentido, inicializarlo con ceros
                if sentido not in datos_por_sentido:
                    datos_por_sentido[sentido] = [0] * 7

                # Asignar la cantidad al día correspondiente
                datos_por_sentido[sentido][indice] = cantidad

            # Preparar las series para la respuesta
            series = []
            colores = [
                "#4f46e5", "#7c3aed", "#0891b2", "#15803d", "#ca8a04",
                "#dc2626", "#ea580c", "#db2777", "#8b5cf6", "#84cc16"
            ]

            # Calcular el total por día
            total_por_dia = [0] * 7

            for idx, (sentido, datos_dias) in enumerate(datos_por_sentido.items()):
                series.append(SerieDatos(
                    nombre=sentido,
                    datos=datos_dias,
                    color=colores[idx % len(colores)]
                ))

                # Sumar al total por día
                total_por_dia = [a + b for a, b in zip(total_por_dia, datos_dias)]

            # Formatar las etiquetas de días
            etiquetas = dias_semana.copy()

            # Incluir el número de semana ISO y rango de fechas en la primera etiqueta
            etiquetas[0] = f"Semana {semana_iso} ({rango_fechas}) - {etiquetas[0]}"

            # Agregar la serie de "Total" al inicio
            series.insert(0, SerieDatos(
                nombre="Total",
                datos=total_por_dia,
                color="#6366f1"
            ))

            cursor.close()
            return GraficoDetalladoResponse(
                etiquetas=etiquetas,
                series=series,
                total=sum(total_por_dia)
            )

        # NUEVA LÓGICA ESPECÍFICA PARA PERÍODO PERSONALIZADO EN GRÁFICO DETALLADO
        elif periodo == "personalizado":
            # Obtener parámetro de agrupación (por defecto 'dia')
            agrupar_por_param = agrupar_por if agrupar_por else 'dia'

            # Validar fechas
            if not fecha_inicio or not fecha_fin:
                cursor.close()
                return GraficoDetalladoResponse(etiquetas=[], series=[], total=0)

            # Calcular diferencia de días para validaciones
            dias_diferencia = calcular_diferencia_dias(fecha_inicio, fecha_fin)

            # Validar agrupación por hora (solo para rangos ≤ 7 días)
            if agrupar_por_param == 'hora' and dias_diferencia > 7:
                agrupar_por_param = 'dia'  # Cambiar a día si el rango es muy grande

            # Construir consulta base
            query_base = """
                SELECT
                    fecha_lectura,
                    sentido_lectura,
                    COUNT(*) AS total_lecturas
                FROM LECTURAS
                WHERE DATE(fecha_lectura) >= %s AND DATE(fecha_lectura) <= %s
            """
            params = [fecha_inicio, fecha_fin]

            # Añadir filtros de hora si existen
            if hora_inicio:
                query_base += " AND TIME(fecha_lectura) >= %s"
                params.append(hora_inicio)
            if hora_fin:
                query_base += " AND TIME(fecha_lectura) <= %s"
                params.append(hora_fin)

            # Aplicar filtros adicionales
            if comuna_id is not None:
                cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
                comunas = cursor.fetchall()
                if comunas and 0 <= comuna_id < len(comunas):
                    query_base += " AND comuna = %s"
                    params.append(comunas[comuna_id]['comuna'])

            if ubicacion_id is not None:
                query_base += " AND id_ubicacion = %s"
                params.append(ubicacion_id)

            if sentidos:
                sentidos_list = [int(s.strip()) for s in sentidos.split(',') if s.strip().isdigit()]
                if sentidos_list:
                    sentidos_placeholders = ", ".join(["%s"] * len(sentidos_list))
                    sentidos_query = f"""
                        SELECT sentido_lectura
                        FROM SENTIDOS_SENSOR
                        WHERE id IN ({sentidos_placeholders})
                    """
                    sentidos_cursor = db.cursor(dictionary=True)
                    sentidos_cursor.execute(sentidos_query, sentidos_list)
                    sentidos_valores = [row['sentido_lectura'] for row in sentidos_cursor.fetchall()]
                    sentidos_cursor.close()

                    if sentidos_valores:
                        sentidos_valores_placeholders = ", ".join(["%s"] * len(sentidos_valores))
                        query_base += f" AND sentido_lectura IN ({sentidos_valores_placeholders})"
                        params.extend(sentidos_valores)

            # Aplicar agrupación según el parámetro
            if agrupar_por_param == 'hora':
                query = query_base + " GROUP BY HOUR(fecha_lectura), sentido_lectura ORDER BY HOUR(fecha_lectura), sentido_lectura"
                etiquetas = generar_etiquetas_horas()
                num_elementos = 24

            elif agrupar_por_param == 'dia':
                query = query_base + " GROUP BY DATE(fecha_lectura), sentido_lectura ORDER BY DATE(fecha_lectura), sentido_lectura"
                etiquetas = generar_etiquetas_dias(fecha_inicio, fecha_fin)
                num_elementos = len(etiquetas)

            elif agrupar_por_param == 'semana':
                query = query_base + " GROUP BY YEARWEEK(fecha_lectura, 1), sentido_lectura ORDER BY YEARWEEK(fecha_lectura, 1), sentido_lectura"
                etiquetas = generar_etiquetas_semanas(fecha_inicio, fecha_fin)
                num_elementos = len(etiquetas)

            elif agrupar_por_param == 'mes':
                query = query_base + " GROUP BY YEAR(fecha_lectura), MONTH(fecha_lectura), sentido_lectura ORDER BY YEAR(fecha_lectura), MONTH(fecha_lectura), sentido_lectura"
                etiquetas = generar_etiquetas_meses(fecha_inicio, fecha_fin)
                num_elementos = len(etiquetas)

            else:
                # Por defecto usar agrupación por día
                query = query_base + " GROUP BY DATE(fecha_lectura), sentido_lectura ORDER BY DATE(fecha_lectura), sentido_lectura"
                etiquetas = generar_etiquetas_dias(fecha_inicio, fecha_fin)
                num_elementos = len(etiquetas)

            cursor.execute(query, params)
            resultados = cursor.fetchall()

            # Procesar resultados por sentido
            datos_por_sentido = {}

            for row in resultados:
                sentido = row['sentido_lectura'] or 'Sin sentido'
                fecha_lectura = row['fecha_lectura']
                cantidad = row['total_lecturas']

                # Inicializar sentido si no existe
                if sentido not in datos_por_sentido:
                    datos_por_sentido[sentido] = [0] * num_elementos

                # Determinar índice según el tipo de agrupación
                if agrupar_por_param == 'hora':
                    indice = datetime.fromisoformat(str(fecha_lectura)).hour
                elif agrupar_por_param == 'dia':
                    fecha = datetime.fromisoformat(str(fecha_lectura)).date()
                    fecha_inicio_dt = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
                    indice = (fecha - fecha_inicio_dt).days
                elif agrupar_por_param == 'semana':
                    # Calcular índice de semana (simplificado)
                    fecha = datetime.fromisoformat(str(fecha_lectura)).date()
                    fecha_inicio_dt = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
                    indice = (fecha - fecha_inicio_dt).days // 7
                elif agrupar_por_param == 'mes':
                    fecha = datetime.fromisoformat(str(fecha_lectura)).date()
                    fecha_inicio_dt = datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
                    indice = (fecha.year - fecha_inicio_dt.year) * 12 + (fecha.month - fecha_inicio_dt.month)
                else:
                    indice = 0

                # Asignar cantidad si el índice es válido
                if 0 <= indice < num_elementos:
                    datos_por_sentido[sentido][indice] = cantidad

            # Preparar las series para la respuesta
            series = []
            colores = [
                "#4f46e5", "#7c3aed", "#0891b2", "#15803d", "#ca8a04",
                "#dc2626", "#ea580c", "#db2777", "#8b5cf6", "#84cc16"
            ]

            # Calcular el total por elemento
            total_por_elemento = [0] * num_elementos

            for idx, (sentido, datos_elementos) in enumerate(datos_por_sentido.items()):
                series.append(SerieDatos(
                    nombre=sentido,
                    datos=datos_elementos,
                    color=colores[idx % len(colores)]
                ))

                # Sumar al total por elemento
                total_por_elemento = [a + b for a, b in zip(total_por_elemento, datos_elementos)]

            # Agregar la serie de "Total" al inicio
            series.insert(0, SerieDatos(
                nombre="Total",
                datos=total_por_elemento,
                color="#6366f1"
            ))

            cursor.close()
            return GraficoDetalladoResponse(
                etiquetas=etiquetas,
                series=series,
                total=sum(total_por_elemento)
            )

        # Procesamiento original para período HOY
        etiquetas = [f"{h:02d}:00 - {h:02d}:59" for h in range(24)]
        series = []
        total = 0

        query = """
            SELECT
                HOUR(fecha_lectura) AS hora,
                sentido_lectura,
                COUNT(*) AS total
            FROM LECTURAS
            WHERE DATE(fecha_lectura) = CURDATE()
        """
        params = []

        if comuna_id is not None:
            cursor.execute("SELECT DISTINCT comuna FROM UBICACIONES ORDER BY comuna")
            comunas = cursor.fetchall()
            if comunas and 0 <= comuna_id < len(comunas):
                query += " AND comuna = %s"
                params.append(comunas[comuna_id]['comuna'])

        if ubicacion_id is not None:
            query += " AND id_ubicacion = %s"
            params.append(ubicacion_id)

        query += """
            GROUP BY sentido_lectura, HOUR(fecha_lectura)
            ORDER BY sentido_lectura, HOUR(fecha_lectura)
        """

        cursor.execute(query, params)
        resultados = cursor.fetchall()

        datos_por_sentido = {}
        for row in resultados:
            sentido = row['sentido_lectura']
            hora = int(row['hora'])
            cantidad = row['total']
            if sentido not in datos_por_sentido:
                datos_por_sentido[sentido] = [0] * 24
            datos_por_sentido[sentido][hora] = cantidad

        colores = [
            "#4f46e5", "#7c3aed", "#0891b2", "#15803d", "#ca8a04",
            "#dc2626", "#ea580c", "#db2777", "#8b5cf6", "#84cc16"
        ]

        total_por_hora = [0] * 24

        for idx, (sentido, datos_horas) in enumerate(datos_por_sentido.items()):
            series.append(SerieDatos(
                nombre=sentido,
                datos=datos_horas,
                color=colores[idx % len(colores)]
            ))
            total_por_hora = [a + b for a, b in zip(total_por_hora, datos_horas)]

        total = sum(total_por_hora)

        series.insert(0, SerieDatos(nombre="Total", datos=total_por_hora, color="#6366f1"))

        cursor.close()

        return GraficoDetalladoResponse(
            etiquetas=etiquetas,
            series=series,
            total=total
        )
    except Exception as e:
        import traceback
        print("Error:", str(e))
        print(traceback.format_exc())
        return GraficoDetalladoResponse(etiquetas=[], series=[], total=0)
