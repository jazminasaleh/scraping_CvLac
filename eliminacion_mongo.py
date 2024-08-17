from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

MONGO_URI = "mongodb+srv://jazminasaleh:IRdNyaCqKVvHyhZ3@gruposinvestigacion.gpd7xka.mongodb.net/"

try:
    client = MongoClient(MONGO_URI)
    db = client.grupos_investigacion  # Nombre de la base de datos
    collection = db.grupos  # Nombre de la colección
    
    # Eliminar todos los documentos de la colección
    result = collection.delete_many({})
    
    print(f"Se eliminaron {result.deleted_count} documentos de la colección 'grupos'")
    print("Conexión a MongoDB Atlas establecida con éxito")
except ConnectionFailure:
    print("No se pudo conectar a MongoDB Atlas")

finally:
  client.close()