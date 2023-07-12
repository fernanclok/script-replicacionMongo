from pymongo import MongoClient
import subprocess
import time
import os

# Configuración de la conexión a MongoDB Atlas // Configuration of the connection to MongoDB Atlas
atlas_connection_string = "mongodb+srv://PruebaWeb:pruebaweb@cluster0.lolgdgf.mongodb.net/?retryWrites=true&w=majority"
atlas_client = MongoClient(atlas_connection_string)
atlas_db = atlas_client["test"] # nombre de la base de datos // name of the database
atlas_collection = atlas_db["users"] # nombre de la coleccion // name of the collection

# Configuración de la conexión a MongoDB Compass // Configuration of the connection to MongoDB Compass
compass_connection_string = "mongodb://127.0.0.1:27017"
compass_client = MongoClient(compass_connection_string)
compass_db = compass_client["replicacion1"] # nombre de la base de datos // name of the database
compass_collection = compass_db["replicas"] # nombre de la coleccion // name of the collection

# Función para replicar los datos // function to replicate our data
def replicate_data():
    # Obtener los documentos de MongoDB Atlas // get all the documents form MongoDB Atlas
    documents = list(atlas_collection.find())

    if len(documents) > 0:
        # Exportar los datos a un archivo de respaldo // export the data to a backup file
        backup_file = "replica.txt" # nombre de nuestro archivo donde se guardara los datos // name our your file when whe are exportin the data 
        subprocess.run(["mongodump", "--uri", atlas_connection_string, "--archive=" + backup_file])

        # Verificar si el archivo de respaldo se generó correctamente // validate if the backup file exists
        if os.path.exists(backup_file) and os.path.getsize(backup_file) > 0:
            print("El archivo de respaldo se generó correctamente.")

            # Importar los datos en MongoDB Compass // import the data to MongoDB Compass
            subprocess.run(["mongorestore", "--uri", compass_connection_string, "--archive=" + backup_file, "--nsFrom=test.users", "--nsTo=replicacion1.replicas"])
            print("Los datos se importaron correctamente en MongoDB Compass.")
        else:
            print("Hubo un problema al generar el archivo de respaldo.")
    else:
        print("No hay nuevos documentos para replicar.")

# Replicar los datos cada cierto intervalo de tiempo (por ejemplo, cada 2 minutos) // replicate the data every 2 minutes
while True:
    replicate_data()
    time.sleep(2 * 60)  # Esperar 2 minutos antes de la siguiente replicación // wait 2 minutes and do it the replication again
