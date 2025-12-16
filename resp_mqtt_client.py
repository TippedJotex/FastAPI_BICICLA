from gmqtt import Client as MQTTClient
import asyncio
import json
from mysql.connector.connection import MySQLConnection
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde la ubicación correcta
load_dotenv("/home/ubuntu/FastAPI_BICICLA/.env")

# Configuración desde variables de entorno
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "bicicla_client")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")

# Cliente MQTT
client = MQTTClient(MQTT_CLIENT_ID)

# Pool de conexiones a la base de datos
from app.database import connection_pool

# --- Conexión y reconexión MQTT ---
def on_connect(client, flags, rc, properties):
    print("✅ Conectado a MQTT Broker")
    client.subscribe('Bramal/Bicicla/#', qos=1)

def on_disconnect(client, packet, exc=None):
    print("⚠️ Desconectado del broker MQTT")
    asyncio.create_task(reconnect_loop())

async def reconnect_loop():
    delay = 3
    while True:
        try:
            print(f"🔄 Reintentando conexión en {delay}s...")
            await asyncio.sleep(delay)
            await client.connect(MQTT_BROKER, MQTT_PORT)
            break
        except Exception as e:
            print(f"❌ Falló reconexión: {e}")
            delay = min(delay + 2, 30)

# --- Procesamiento de mensajes ---
def on_message(client, topic, payload, qos, properties):
    conn = None
    cursor = None
    
    try:
        parts = topic.split("/")
        if len(parts) != 6:
            print(f"❌ Tópico inválido: {topic}")
            return

        _, _, tipo_equipo, comuna, ubicacion_endpoint, sensor = parts
        sensor = sensor.strip().upper()

        # Verificación preliminar - si no es sensor BC, solo ignoramos
        if not sensor.startswith("BC"):
            print(f"⚠️ Ignorando mensaje: Sensor {sensor} no es de tipo BC")
            return

        # Solo obtenemos conexión a DB para sensores que procesaremos
        conn = connection_pool.get_connection()
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

        # 5. Insertar lectura (siempre se guardan todas las lecturas como histórico)
        cursor.execute("""
            INSERT INTO LECTURAS 
            (NOMBRE_SENSOR, ID_UBICACION, COMUNA, UBICACION_ENDPOINT, DIRECCION, SENTIDO_LECTURA) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (sensor, id_ubicacion, comuna, ubicacion_endpoint, direction, sentido_lectura))

        # Confirmar transacción
        conn.commit()
        print(f"✅ Procesamiento completo: Sensor {sensor} - Dirección: {direction}")
        
    except Exception as e:
        # En caso de error, revertir toda la transacción
        if conn is not None:
            try:
                conn.rollback()
            except:
                pass
        print(f"❌ ERROR en procesamiento: {str(e)}")
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

# --- Inicializar conexión MQTT ---
async def connect_mqtt():
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    if MQTT_USER and MQTT_PASSWORD:
        client.set_auth_credentials(MQTT_USER, MQTT_PASSWORD)

    print(f"⏳ Iniciando conexión MQTT a {MQTT_BROKER}:{MQTT_PORT}...")
    await client.connect(MQTT_BROKER, MQTT_PORT)
    print(f"🔌 Conexión MQTT establecida correctamente")
    return client

# Función para iniciar el cliente MQTT en background
async def start_mqtt_client():
    try:
        await connect_mqtt()
        # Mantener la conexión activa
        await asyncio.Event().wait()
    except Exception as e:
        print(f"❌ Error en el cliente MQTT: {str(e)}")

# --- Para pruebas directas ---
if __name__ == "__main__":
    asyncio.run(start_mqtt_client())
