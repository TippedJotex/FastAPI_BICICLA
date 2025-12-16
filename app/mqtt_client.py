from gmqtt import Client as MQTTClient
import asyncio
import json
from mysql.connector.connection import MySQLConnection
import os
import sys
import time
from dotenv import load_dotenv
import datetime

# Cargar variables de entorno desde la ubicación correcta en el servidor de producción
load_dotenv("/home/ubuntu/FastAPI_BICICLA/.env")

# Configuración desde variables de entorno
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")

# Configuración de reintentos
MAX_DB_RETRY_DELAY = 10    # Máximo tiempo entre reintentos para DB (segundos)
MAX_MQTT_RETRY_DELAY = 5   # Máximo tiempo entre reintentos para MQTT (segundos)
CRITICAL_FAILURE_COUNT = 10 # Después de estos fallos, reinicia todo el proceso

# Contadores de fallos
db_failure_count = 0
mqtt_failure_count = 0

reconnecting = False



# Cliente MQTT
#client = MQTTClient(MQTT_CLIENT_ID)

base_id = os.getenv("MQTT_CLIENT_ID", "bicicla-backend")
MQTT_CLIENT_ID = f"{base_id}-{os.getpid()}"
client = MQTTClient(MQTT_CLIENT_ID)
print(f"🆔 Usando MQTT_CLIENT_ID: {MQTT_CLIENT_ID}")





# Pool de conexiones a la base de datos
from app.database import connection_pool

# --- Reinicio de aplicación ---
def restart_application():
    """Reinicia completamente la aplicación en caso de fallos graves"""
    print("🔄 REINICIANDO APLICACIÓN COMPLETA...")
    python = sys.executable
    os.execv(python, [python] + sys.argv)
    # Esta función será reemplazada por main.py para manejar reinicio seguro

# --- Manejo de base de datos ---
def get_db_connection():
    """Obtiene una conexión a la base de datos con reintentos"""
    global db_failure_count
    delay = 1
    
    while True:
        try:
            conn = connection_pool.get_connection()
            db_failure_count = 0  # Reiniciar contador al tener éxito
            return conn
        except Exception as e:
            db_failure_count += 1
            print(f"❌ Error al conectar a la base de datos (intento {db_failure_count}): {e}")
            
            if db_failure_count >= CRITICAL_FAILURE_COUNT:
                print("❗ FALLO CRÍTICO: Demasiados errores de conexión a la base de datos")
                restart_application()
                
            print(f"🔄 Reintentando conexión a DB en {delay}s...")
            time.sleep(delay)
            delay = min(delay * 1.5, MAX_DB_RETRY_DELAY)

# --- Conexión y reconexión MQTT ---
def on_connect(client, flags, rc, properties):
    global mqtt_failure_count
    print("✅ Conectado a MQTT Broker")
    mqtt_failure_count = 0  # Reiniciar contador al tener éxito
    client.subscribe('Bramal/Bicicla/#', qos=1)

def on_disconnect(client, packet, exc=None):
    global mqtt_failure_count
    mqtt_failure_count += 1
    print(f"⚠️ Desconectado del broker MQTT (desconexión {mqtt_failure_count})")
    
    if mqtt_failure_count >= CRITICAL_FAILURE_COUNT:
        print("❗ FALLO CRÍTICO: Demasiadas desconexiones del broker MQTT")
        restart_application()
    else:
        # Lanzar un único bucle de reconexión controlado
        asyncio.create_task(reconnect_loop())

async def reconnect_loop():
    global reconnecting

    # Si ya hay un proceso de reconexión en marcha, no hacer nada
    if reconnecting:
        print("⏳ Ya existe un proceso de reconexión MQTT en curso, se omite uno nuevo.")
        return

    reconnecting = True
    delay = 1

    try:
        while True:
            try:
                print(f"🔄 Reintentando conexión MQTT en {delay}s...")
                await asyncio.sleep(delay)
                await client.connect(MQTT_BROKER, MQTT_PORT)
                print("✅ Re-conexión MQTT establecida correctamente")
                break
            except Exception as e:
                print(f"❌ Falló reconexión MQTT: {e}")
                delay = min(delay * 1.5, MAX_MQTT_RETRY_DELAY)
    finally:
        # Permitir futuros intentos de reconexión
        reconnecting = False


# --- Procesamiento de mensajes ---
def on_message(client, topic, payload, qos, properties):
    conn = None
    cursor = None

    try:
        print(f"📩 Mensaje MQTT recibido: Tópico={topic}, Payload={payload}")

        parts = topic.split("/")

        # MAIV Agregar procesamiento de nuevo tipo de mensaje de Status, tratan los casos len(parts) == 6, y len(parts) == 7 (nuevo). Además se agrega tratamiento y registro de Fecha_Real y Hora_Real
        ##if len(parts) != 6:
        ##    print(f"❌ Tópico inválido: {topic}")
        ##    return

        # Estos mensajes de status mormales tienen una estructura de tópico diferente (6 partes).
        if len(parts) == 6:        
            
            # MAIV Comentar temporalmente para prueba local
            
            _, _, tipo_equipo, comuna, ubicacion_endpoint, sensor = parts
            sensor = sensor.strip().upper()

            # Verificación preliminar - si no es sensor BC, solo ignoramos
            if not sensor.startswith("BC"):
                print(f"⚠️ Ignorando mensaje: Sensor {sensor} no es de tipo BC")
                return

            # Solo obtenemos conexión a DB para sensores que procesaremos
            # Usamos nuestra función mejorada con reintentos
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)

            # Iniciar transacción para evitar problemas de concurrencia
            conn.start_transaction()

            print(f"📥 Procesando: {tipo_equipo} - {comuna} - {ubicacion_endpoint} - {sensor}")

            # 1. Verificar si la ubicación ya existe
            cursor.execute("""
                SELECT ID_UBICACION FROM UBICACIONES
                WHERE UBICACION_ENDPOINT = %s
            """, (ubicacion_endpoint,))
            ubicacion_row = cursor.fetchone()

            if ubicacion_row:
                # Usar ID existente
                id_ubicacion = ubicacion_row["ID_UBICACION"]
                print(f"📍 Usando ubicación existente (ID: {id_ubicacion})")

                # Asegurar que los datos estén actualizados
                cursor.execute("""
                    UPDATE UBICACIONES
                    SET COMUNA = %s, TIPO_EQUIPO = %s
                    WHERE ID_UBICACION = %s
                """, (comuna, tipo_equipo, id_ubicacion))
            else:
                # Insertar nueva ubicación
                cursor.execute("""
                    INSERT INTO UBICACIONES
                    (COMUNA, UBICACION_ENDPOINT, TIPO_EQUIPO)
                    VALUES (%s, %s, %s)
                """, (comuna, ubicacion_endpoint, tipo_equipo))

                # Obtener ID de la ubicación recién insertada
                cursor.execute("SELECT LAST_INSERT_ID() as ID_UBICACION")
                id_ubicacion = cursor.fetchone()["ID_UBICACION"]
                print(f"🆕 Nueva ubicación creada (ID: {id_ubicacion})")

            # 2. Verificar si el sensor ya existe para esta ubicación
            cursor.execute("""
                SELECT ID_SENSOR FROM SENSORES
                WHERE NOMBRE_SENSOR = %s AND ID_UBICACION = %s
            """, (sensor, id_ubicacion))
            sensor_row = cursor.fetchone()

            if sensor_row:
                # Usar ID existente
                id_sensor = sensor_row["ID_SENSOR"]
                print(f"🔍 Usando sensor existente (ID: {id_sensor})")
            else:
                # Insertar nuevo sensor
                cursor.execute("""
                    INSERT INTO SENSORES
                    (NOMBRE_SENSOR, ID_UBICACION)
                    VALUES (%s, %s)
                """, (sensor, id_ubicacion))

                # Obtener ID del sensor recién insertado
                cursor.execute("SELECT LAST_INSERT_ID() as ID_SENSOR")
                id_sensor = cursor.fetchone()["ID_SENSOR"]
                print(f"🆕 Nuevo sensor creado (ID: {id_sensor})")

            # 3. Obtener información de dirección del payload
            try:
                data = json.loads(payload)
                direction = data.get("direction", "Desconocido")
            except:
                direction = "Desconocido"
                print("⚠️ Error al parsear payload JSON, usando dirección 'Desconocido'")

            # 3.1 Obtener información de la Fecha y Hora real, si es un mensaje de sincronización
            
            #MAIV: Agregar campos de tiempo real
            fecha_real = ""
            hora_real = ""
            try:
                data = json.loads(payload)
                fecha_real = data.get("reading_date", "")
                hora_real = data.get("reading_time", "")
                print(f"🔍 El mensaje es de sincronización ID: {id_sensor}, reading_date: '{fecha_real}', reading_time: '{hora_real}' ")
            except:
                print("⚠️ El mensaje No es de sincronización")
            

            # 4. Verificar si ya existe el registro de sentido para este sensor
            cursor.execute("""
                SELECT ID, SENTIDO_LECTURA FROM SENTIDOS_SENSOR
                WHERE ID_SENSOR = %s AND DIRECCION = %s
            """, (id_sensor, direction))
            sentido_row = cursor.fetchone()

            if sentido_row:
                # Usar sentido existente
                sentido_lectura = sentido_row["SENTIDO_LECTURA"]
                print(f"🔍 Usando sentido existente: {sentido_lectura if sentido_lectura else 'NULL'}")
            else:
                # Insertar nuevo sentido
                cursor.execute("""
                    INSERT INTO SENTIDOS_SENSOR
                    (ID_SENSOR, DIRECCION, SENTIDO_LECTURA)
                    VALUES (%s, %s, NULL)
                """, (id_sensor, direction))
                sentido_lectura = None
                print(f"🆕 Nuevo sentido creado para dirección: {direction}")

            # 5. Verificar la estructura de la tabla LECTURAS
            cursor.execute("DESCRIBE BICICLA.LECTURAS")
            columnas_lecturas = cursor.fetchall()
            print(f"Estructura de tabla LECTURAS: {[col['Field'] for col in columnas_lecturas]}")

            # Comprobar si existe campo FECHA_LECTURA
            tiene_fecha_lectura = any(col['Field'] == 'FECHA_LECTURA' for col in columnas_lecturas)

            # 6. Insertar lectura (siempre se guardan todas las lecturas como histórico)
            # MODIFICADO: Ahora incluimos ID_SENSOR en la inserción
            print(f"Insertando lectura: {sensor}, ID_UBICACION: {id_ubicacion}, ID_SENSOR: {id_sensor}")

            if tiene_fecha_lectura:
                # Si la tabla tiene un campo FECHA_LECTURA, lo incluimos
                
                #MAIV: Agregar campos de tiempo real
                
                
                valsTiempoReal = ""
                
                cmpsTiempoReal = ", FECHA_REAL, HORA_REAL"
                
                if fecha_real!="":
                    valsTiempoReal = ", %s, %s"
                else:
                    valsTiempoReal = ", CURDATE(),  CURTIME()"
                    
                
                SqlInsert = f"""
                    INSERT INTO LECTURAS
                    (NOMBRE_SENSOR, ID_UBICACION, ID_SENSOR, COMUNA, UBICACION_ENDPOINT, DIRECCION, SENTIDO_LECTURA, FECHA_LECTURA{cmpsTiempoReal})
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(){valsTiempoReal})
                """
                if fecha_real!="":
                    cursor.execute(SqlInsert, (sensor, id_ubicacion, id_sensor, comuna, ubicacion_endpoint, direction, sentido_lectura, fecha_real, hora_real))
                else:
                    cursor.execute(SqlInsert, (sensor, id_ubicacion, id_sensor, comuna, ubicacion_endpoint, direction, sentido_lectura))
            else:
                # Usar la consulta modificada incluyendo ID_SENSOR
                cursor.execute("""
                    INSERT INTO LECTURAS
                    (NOMBRE_SENSOR, ID_UBICACION, ID_SENSOR, COMUNA, UBICACION_ENDPOINT, DIRECCION, SENTIDO_LECTURA)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (sensor, id_ubicacion, id_sensor, comuna, ubicacion_endpoint, direction, sentido_lectura))

            print(f"Inserción ejecutada, filas afectadas: {cursor.rowcount}")

            # Confirmar transacción
            conn.commit()
            print(f"✅ Procesamiento completo: Sensor {sensor} - Dirección: {direction}")
            
        elif len(parts) == 7 and parts[-2] == "control" and parts[-1] == "status":
            
            #MAIV, tratamiento de registro de status
            
            print("🔄 Procesando mensaje de status de Totem ...")

            # 1. Extraer información del tópico
            _, _, tipo_equipo, comuna, ubicacion_endpoint, _, _ = parts

            # 2. Obtener conexión a la base de datos
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            conn.start_transaction()

            # 3. Buscar el ID de la ubicación
            cursor.execute("""
                SELECT ID_UBICACION FROM UBICACIONES
                WHERE UBICACION_ENDPOINT = %s
            """, (ubicacion_endpoint,))
            ubicacion_row = cursor.fetchone()

            if ubicacion_row:
                id_ubicacion = ubicacion_row["ID_UBICACION"]
                print(f"📍 Ubicación encontrada para status (ID: {id_ubicacion})")
            else:
                # Si la ubicación no existe, la creamos para mantener la consistencia
                cursor.execute("""
                    INSERT INTO UBICACIONES (COMUNA, UBICACION_ENDPOINT, TIPO_EQUIPO)
                    VALUES (%s, %s, %s)
                """, (comuna, ubicacion_endpoint, tipo_equipo))
                cursor.execute("SELECT LAST_INSERT_ID() as ID_UBICACION")
                id_ubicacion = cursor.fetchone()["ID_UBICACION"]
                print(f"🆕 Nueva ubicación creada para status (ID: {id_ubicacion})")

            sValorDashEnabled = "0"
            
            # 4. Parsear el payload del mensaje de status
            try:
                data = json.loads(payload)
                dashboard_enabled = data.get("dashboard_enabled", False)
                
                if dashboard_enabled:
                    sValorDashEnabled = "1"
                
                mqtt_connected = data.get("mqtt_connected", False)

                raw_timestamp = data.get("timestamp")
                device = data.get("device")
                uptime = data.get("uptime")

                if raw_timestamp:
                    # Convertir timestamp ISO 8601 (con 'T') a formato MySQL: 'YYYY-MM-DD HH:MM:SS'
                    #timestamp = raw_timestamp.replace("T", " ")
                    
                    timestamp = raw_timestamp
                else:
                    timestamp = None

                # ✅ Validar campos obligatorios
                if not device or not timestamp:
                    print(f"❌ Campos obligatorios faltantes para status: device={device}, timestamp={timestamp}")
                    if conn: conn.rollback()
                    return

            except Exception as e:
                print(f"❌ Error al parsear payload de status: {e}")
                # Si el payload es inválido, no podemos continuar. Revertimos y salimos.
                if conn: conn.rollback()
                return

            # 5. Insertar el registro de status en la tabla UBICACION_STATUS
            # Se convierten los booleanos a enteros (1/0) para la BD.
            # Se convierte uptime a string para la BD.
            
            ''' Cambiar a campo SDASHBOARD_ENABLED, dejar el default en DASHBOARD_ENABLED (1)
            sql_insert_status = """
                INSERT INTO UBICACION_STATUS
                (ID_UBICACION, DASHBOARD_ENABLED, MQTT_CONNECTED, TIMESTAMP, DEVICE, UPTIME, SDASHBOARD_ENABLED)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            '''

            sql_insert_status = """
                INSERT INTO UBICACION_STATUS
                (ID_UBICACION, MQTT_CONNECTED, TIMESTAMP, DEVICE, UPTIME, SDASHBOARD_ENABLED)
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            '''
            cursor.execute(sql_insert_status, (
                id_ubicacion,
                int(dashboard_enabled),
                int(mqtt_connected),
                timestamp,
                device,
                str(uptime),
                dashboard_enabled
            ))
            '''

            cursor.execute(sql_insert_status, (
                id_ubicacion,
                int(mqtt_connected),
                timestamp,
                device,
                str(uptime),
                sValorDashEnabled
            ))

            print(f"Inserción de status ejecutada, filas afectadas: {cursor.rowcount}")

            # Confirmar transacción
            conn.commit()
            print(f"✅ Status procesado correctamente para el dispositivo: {device}")

        else:
            # Si el tópico no tiene 6 ni 7 partes, o no cumple los formatos esperados, se ignora.
            print(f"❌ Tópico con formato no reconocido o irrelevante: {topic}")


    except Exception as e:
        # En caso de error, revertir toda la transacción
        if conn is not None:
            try:
                conn.rollback()
            except:
                pass
        print(f"❌ ERROR en procesamiento: {str(e)}")

        # Información detallada del error para depuración
        import traceback
        print(f"Detalles del error: {traceback.format_exc()}")
    finally:
        # Asegurar que los recursos se liberen
        if cursor is not None:
            try:
                cursor.close()
            except:
                pass
        if conn is not None:
            try:
                conn.close()
            except:
                pass

# --- Inicializar conexión MQTT con manejo mejorado ---
async def connect_mqtt():
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    if MQTT_USER and MQTT_PASSWORD:
        client.set_auth_credentials(MQTT_USER, MQTT_PASSWORD)

    # Mejorado: bucle de reconexión integrado
    while True:
        try:
            print(f"⏳ Iniciando conexión MQTT a {MQTT_BROKER}:{MQTT_PORT}...")
            await client.connect(MQTT_BROKER, MQTT_PORT)
            print(f"🔌 Conexión MQTT establecida correctamente")
            return client
        except Exception as e:
            print(f"❌ Error al conectar con MQTT: {str(e)}")
            # Esperamos antes de reintentar
            delay = 1
            print(f"🔄 Reintentando conexión MQTT inicial en {delay}s...")
            await asyncio.sleep(delay)

# Función para iniciar el cliente MQTT en background con mejor manejo de errores
async def start_mqtt_client():
    try:
        await connect_mqtt()
        # Bucle infinito para mantener la conexión y reiniciar si es necesario
        while True:
            try:
                # Esperar indefinidamente mientras la conexión está activa
                await asyncio.Event().wait()
            except Exception as e:
                print(f"⚠️ Error en el bucle principal: {str(e)}")
                import traceback
                print(f"Detalles: {traceback.format_exc()}")
                # Si llegamos aquí, ha ocurrido un error inesperado en el bucle principal
                # Esperamos un momento y reiniciamos todo
                await asyncio.sleep(2)
                restart_application()
    except Exception as e:
        print(f"❌ Error crítico en el cliente MQTT: {str(e)}")
        import traceback
        print(f"Detalles del error crítico: {traceback.format_exc()}")
        # Error no recuperable, reiniciamos todo
        restart_application()

# --- Manejador de señales para cierre seguro ---
def handle_exit_signals():
    import signal

    def signal_handler(sig, frame):
        print("\n⚠️ Señal de interrupción recibida. Cerrando conexiones...")
        asyncio.create_task(client.disconnect())
        # Dar tiempo para desconectar correctamente
        time.sleep(1)
        sys.exit(0)

    # Registrar manejadores para señales comunes
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

# --- Para pruebas directas ---
if __name__ == "__main__":
    handle_exit_signals()
    print("🚀 Iniciando servicio de monitoreo de bicicletas BICICLA")
    print("📊 Este servicio registra cada bicicleta detectada por los sensores")
    asyncio.run(start_mqtt_client())
