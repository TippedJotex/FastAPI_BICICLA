import os
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv
from fastapi import Depends
from typing import Generator

# Cargar variables de entorno desde .env
load_dotenv("/home/ubuntu/FastAPI_BICICLA/.env")

# Obtener configuración de base de datos desde variables de entorno
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "BICICLA")

# Configuración del pool de conexiones
connection_pool = pooling.MySQLConnectionPool(
    pool_name="bicicletas_pool",
    pool_size=5,
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB,
    autocommit=True  # Recomendado para operaciones de lectura
)

# Función para obtener una conexión del pool
def get_db() -> Generator[mysql.connector.connection.MySQLConnection, None, None]:
    """
    Provee una conexión de base de datos desde el pool.
    La conexión se cierra automáticamente después de su uso.
    """
    conn = None
    try:
        conn = connection_pool.get_connection()
        yield conn
    finally:
        if conn is not None and conn.is_connected():
            conn.close()
