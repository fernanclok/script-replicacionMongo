from pymongo import MongoClient
import subprocess
import time
import os

# Configuración de la conexión a MongoDB Atlas
atlas_connection_string = "mongodb+srv://PruebaWeb:pruebaweb@cluster0.lolgdgf.mongodb.net/?retryWrites=true&w=majority"
atlas_client = MongoClient(atlas_connection_string)
atlas_db = atlas_client["test"]
atlas_collection = atlas_db["users"]

# Configuración de la conexión a MongoDB Compass
compass_connection_string = "mongodb://127.0.0.1:27017"
compass_client = MongoClient(compass_connection_string)
compass_db = compass_client["replicacion1"]
compass_collection = compass_db["replicas"]

# Función para replicar los datos
def replicate_data():
    # Obtener los documentos de MongoDB Atlas
    documents = list(atlas_collection.find())

    if len(documents) > 0:
        # Exportar los datos a un archivo de respaldo
        backup_file = "replica.txt"
        subprocess.run(["mongodump", "--uri", atlas_connection_string, "--archive=" + backup_file])

        # Verificar si el archivo de respaldo se generó correctamente
        if os.path.exists(backup_file) and os.path.getsize(backup_file) > 0:
            print("El archivo de respaldo se generó correctamente.")

            # Importar los datos en MongoDB Compass
            subprocess.run(["mongorestore", "--uri", compass_connection_string, "--archive=" + backup_file, "--nsFrom=test.users", "--nsTo=replicacion1.replicas"])
            print("Los datos se importaron correctamente en MongoDB Compass.")
        else:
            print("Hubo un problema al generar el archivo de respaldo.")
    else:
        print("No hay nuevos documentos para replicar.")

# Replicar los datos cada cierto intervalo de tiempo (por ejemplo, cada 5 minutos)
while True:
    replicate_data()
    time.sleep(2 * 60)  # Esperar 2 minutos antes de la siguiente replicación
