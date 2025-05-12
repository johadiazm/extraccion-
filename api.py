
# filepath: c:\extraccion-\api.py
from flask import Flask, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# Conexión a MongoDB
client = MongoClient('mongodb+srv://johadiazm11:vxkrVr9yRrhkX7rN@cluster0.1b7nqcb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client['Data_Team']
collection = db['Team']

# Endpoint para obtener todos los datos completos
@app.route('/datos', methods=['GET'])
def obtener_datos():
    datos = list(collection.find({}, {'_id': 0}))  # Devuelve toda la información de la colección
    return jsonify(datos)

# Endpoint para obtener los miembros
@app.route('/miembros', methods=['GET'])
def obtener_miembros():
    miembros = []
    grupos = list(collection.find({}, {'_id': 0, 'nombre_grupo': 1, 'miembros': 1}))
    for grupo in grupos:
        nombre_grupo = grupo.get('nombre_grupo', 'Sin Nombre')
        for miembro in grupo.get('miembros', []):
            miembros.append({
                'nombre_grupo': nombre_grupo,
                'nombre_miembro': miembro.get('Nombre del integrante', 'Sin Nombre'),
                'estado': miembro.get('Estado', 'Desconocido')
            })
    return jsonify(miembros)

# Endpoint para obtener las publicaciones
@app.route('/publicaciones', methods=['GET'])
def obtener_publicaciones():
    publicaciones = []
    grupos = list(collection.find({}, {'_id': 0, 'nombre_grupo': 1, 'publicaciones': 1}))
    for grupo in grupos:
        nombre_grupo = grupo.get('nombre_grupo', 'Sin Nombre')
        for publicacion in grupo.get('publicaciones', []):
            publicaciones.append({
                '_id': publicacion.get('_id', 'Desconocido'),
                'nombre_grupo': nombre_grupo,
                'titulo': publicacion.get('Título', 'Sin Título'),
                'tipo': publicacion.get('Tipo', 'Desconocido'),
                'tipo_publicacion': publicacion.get('Tipo Publicación', 'Desconocido'),
                'revista': publicacion.get('Revista', 'Desconocida'),
                'pais': publicacion.get('País', 'Desconocido'), 
                'ISSN': publicacion.get('ISSN', 'Desconocido'),
                'año': publicacion.get('Año', 'Desconocido'),
                'volumen': publicacion.get('Volumen', 'Desconocido'),
                'autores': publicacion.get('Autores', 'Desconocido'),
                'avalado': publicacion.get('avalado', 'Desconocido'),
                'todo': publicacion.get('todo', 'Desconocido'),
                'DOI': publicacion.get('DOI', 'Desconocido'),
            })
    return jsonify(publicaciones)

if __name__ == '__main__':
    app.run(debug=True)