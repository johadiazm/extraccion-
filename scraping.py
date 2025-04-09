import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry 
import urllib3
from bs4 import BeautifulSoup, SoupStrainer
import os
from concurrent.futures import ThreadPoolExecutor
import re

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


from openpyxl.utils import get_column_letter
from datetime import datetime

# Configurar la conexión a MongoDB
client = MongoClient('mongodb+srv://andressanabria02:uL3Bgc9CCAHiOrgD@cluster0.p02ar.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client['Data_Team']
grupos_collection = db['Team']
miembros_collection = db['members']

# Desactivar las advertencias de solicitudes inseguras (solo para pruebas)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurar una estrategia de reintentos personalizada
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504,443],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    backoff_factor=1
)

# Crear una sesión personalizada con la estrategia de reintentos
session = requests.Session()
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Establecer el tiempo de espera de la sesión
session.timeout = 30
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''

# URL del GrupLac, se toman los 152 grupos
url = 'https://scienti.minciencias.gov.co/ciencia-war/busquedaGrupoXInstitucionGrupos.do?codInst=930&sglPais=&sgDepartamento=&maxRows=152&grupos_tr_=true&grupos_p_=1&grupos_mr_=152'






# Los resultados se van a almacenar en un csv con nombre resultados_grupos
archivo_salida_json = 'resultados_grupos_json.json'
archivo_salida_excel = 'resultados_grupos.xlsx'
archivo_salida_excel_miembros = 'miembros_grupos.xlsx'

def procesar_grupo(fila):
    columnas = fila.find_all('td')

    # Verificar si hay más de tres columnas en la fila
    if len(columnas) >= 3:
        tercer_td = columnas[2]
        

        # Se obtiene el enlace del GrupLAC
        enlace_grupo = tercer_td.find('a')

        # Verificar si se encontró un enlace dentro del tercer <td>
        if enlace_grupo:
            # Extraer el texto (nombre del grupo) y el enlace (gruplac)
            nombre_grupo = enlace_grupo.text.strip()
            href_enlace = enlace_grupo.get('href')
            numero_url = href_enlace.split('=')[-1]
            enlace_gruplac_grupo = f'https://scienti.minciencias.gov.co/gruplac/jsp/visualiza/visualizagr.jsp?nro={numero_url}'

            # Obtener el nombre del líder y el enlace a su CvLac
            nombre_lider = columnas[3].text.strip()
            nombre_lider = nombre_lider.title()
            
            # Devolver los datos del grupo y su líder
            return {
                'enlace_gruplac': enlace_gruplac_grupo,
             
            }

    return None


def extraer_contenido_tabla(tabla, es_lineas_investigacion=False):
    contenido = []
    filas = tabla.find_all('tr')[1:]  # Ignorar la fila del título
    for fila in filas:
        celdas = fila.find_all('td')
        if celdas:
            texto = limpiar_texto(celdas[0].text.strip())
            if es_lineas_investigacion:
                # Eliminar el número, espacio y guión del inicio para líneas de investigación
                texto_limpio = re.sub(r'^\d+\.\s*-\s*', '', texto)
                contenido.append(texto_limpio)
            else:
                contenido.append(texto)
    
    if es_lineas_investigacion:
        return contenido  # Devuelve una lista para líneas de investigación
    else:
        return '\n'.join(contenido)


def info_grupo_publicaciones(link_grupo):
    grupo = {}
    try:
        pedido_obtenido = session.get(link_grupo, verify=False)
        pedido_obtenido.raise_for_status()
        html_obtenido = pedido_obtenido.text
        soup = BeautifulSoup(html_obtenido, "html.parser")

        # Traer el título
        titulo = soup.find_all(class_="celdaEncabezado")
        if titulo:
            grupo["titulo"] = titulo[0].text.strip()

        # Agregar datos básicos
        tablas = soup.find_all('table')
        if tablas:
            primera_tabla = tablas[0]
            filas = primera_tabla.find_all('tr')

            if len(filas) > 0:
                campos = [
                    "Año y mes de formación",
                    "Departamento - Ciudad",
                    "Líder",
                    "Página web",
                    "E-mail",
                    "Clasificación",
                    "Área de conocimiento",
                    "Programa nacional de ciencia y tecnología",
                    "Programa nacional de ciencia y tecnología (secundario)"
                ]

            campo_indice = 0  # Índice manual para la lista de campos

            for i in range(1, len(filas)):
                celdas = filas[i].find_all('td')
                if len(celdas) >= 2:
                    etiqueta = celdas[0].text.strip()

                    # Ignorar la fila si la etiqueta es "¿La información de este grupo se ha certificado?"
                    if etiqueta == "¿La información de este grupo se ha certificado?":
                        continue  # No avanzar el índice del campo

                    # Procesar la fila normalmente
                    valor = celdas[1].text.strip().replace('\xa0', ' ').replace('\r\n', ' ')
                    
                    # Separar departamento y ciudad
                    if campos[campo_indice] == "Departamento - Ciudad":
                        match = re.match(r'^(.*?)\s*-\s*(.*?)$', valor)
                        if match:
                            grupo["Departamento"] = match.group(1).strip()
                            grupo["Ciudad"] = match.group(2).strip()
                        else:
                            grupo["Departamento"] = valor
                            grupo["Ciudad"] = ""
                    else:
                        grupo[campos[campo_indice]] = valor
                    
                    # Avanzar manualmente el índice del campo
                    campo_indice += 1

        # Buscar las tablas específicas
        tablas = soup.find_all('table')
        for tabla in tablas:
            titulo = tabla.find('td', class_='celdaEncabezado')
            if titulo:
                if "Plan Estratégico" in titulo.text:
                    grupo["Plan Estratégico"] = extraer_contenido_tabla(tabla)
                elif "Líneas de investigación declaradas por el grupo" in titulo.text:
                    grupo["Líneas de investigación"] = extraer_contenido_tabla(tabla, es_lineas_investigacion=True)

  

        # Obtener las tablas de "Artículos publicados" y "Otros artículos publicados"
        tablas_validas = []
        titulos_validos = ["Artículos publicados", "Otros artículos publicados", "Libros publicados", "Capítulos de libro publicados"]
        for tabla in tablas:
            primera_fila = tabla.find('tr')
            if primera_fila:
                primera_celda = primera_fila.find('td')
                if primera_celda:
                    texto_celda = primera_celda.get_text(strip=True)
                    if texto_celda in titulos_validos:
                        filas_tabla = tabla.find_all('tr')
                        if len(filas_tabla) > 1:
                            tablas_validas.append(tabla)

        for tabla_valida in tablas_validas:
            primera_fila = tabla_valida.find('tr')
            if primera_fila:
                primera_celda = primera_fila.find('td')
                if primera_celda:
                    titulo_tabla = primera_celda.get_text(strip=True)
                    filas_articulos = tabla_valida.find_all('tr')[1:]  # Ignorar el encabezado
                    
                    # Crear dos categorías para cada tipo de artículo
                    if titulo_tabla not in grupo:
                        grupo[titulo_tabla] = []
                        grupo[f"{titulo_tabla} sin chulo"] = []
                    
                    for fila in filas_articulos:
                        celdas_fila = [limpiar_texto(celda.text) for celda in fila.find_all('td')]
                        if fila.find('img'):
                            grupo[titulo_tabla].append(celdas_fila)
                        else:
                            grupo[f"{titulo_tabla} sin chulo"].append(celdas_fila)
        # Extraer información de los miembros
        grupo['miembros'] = extraer_miembros_grupo(soup, grupo.get('titulo', ''))
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener información del grupo: {e}")

    return grupo

# Función para limpiar texto de caracteres no válidos para Excel
def limpiar_texto(texto):
    
    # Elimina caracteres no imprimibles
    texto = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', texto)
    # Elimina espacios múltiples
    texto = re.sub(r'\s{3,}', ' _ ', texto)
    # Elimina espacios antes de comas y puntos
   
    # Asegura un espacio después de comas y puntos
    texto = re.sub(r'([,.])\s*', r'\1 ', texto)
    # Elimina punto y coma al inicio si existe
    texto = texto.lstrip(';')
   
    return texto.strip()

#expresiones regulares para extraer la información de articulos y otros articulos


def extraer_info_articulo(texto, tipo_publicacion):
    
    info = {
        "Tipo": tipo_publicacion,
        "Tipo Publicación": "",
        "Título": "",
        "Revista": "",
        "País": "",
        "ISSN": "",
        "Año": "",
        "Volumen": "",
        "Fascículo": "",
        "Páginas": "",
        "DOI": "",
        "Autores": [],
        "todo":""
    }

    # Compilar las expresiones regulares para mejorar el rendimiento
    patrones = {
        "Tipo Publicación": re.compile(r"-\s*(.*?):"),
        "Título": re.compile(r"-\s*(?:[^:]+:)?\s*(.*?)\s+_"),
        "Revista": re.compile(r",\s*(.*?)\s*ISSN:"),
        "País": re.compile(r"_[^_]*_\s*([^,]+?)\s*,", re.IGNORECASE),
        "ISSN": re.compile(r"ISSN:\s*(\d{4}-\d{3}[\dX])"),
        "Año": re.compile(r"ISSN:.*?,\s*(\d{4})\s*vol:"),
        "Volumen": re.compile(r"vol:(\d+)"),
        "Fascículo": re.compile(r"fasc:\s*(.*?)\s*págs:"),
        "Páginas": re.compile(r"págs:\s*(\d+\s*-\s*\d+)"),
        "DOI": re.compile(r"DOI:(.*?)Autores:"),
        "Autores": re.compile(r"Autores:\s*(.+)$",re.IGNORECASE),
        "todo": re.compile(r"-.*,(?=[^,]*$)")
    }

    # Extraer información usando las expresiones regulares compiladas
    tipo_match = patrones["Tipo Publicación"].search(texto)
    if tipo_match:
        info["Tipo Publicación"] = tipo_match.group(1).strip()
    
    titulo_match = patrones["Título"].search(texto)
    if titulo_match:
        info["Título"] = titulo_match.group(1).strip()

    revista_match = patrones["Revista"].search(texto)
    if revista_match:
        info["Revista"] = revista_match.group(1).strip()
        
    pais_match = patrones["País"].search(texto)
    if pais_match:
        pais_extraido = pais_match.group(1).strip()
        # Verifica si el país extraído tiene un formato válido
        if pais_extraido and not any(char.isdigit() for char in pais_extraido):
            info["País"] = pais_extraido
        else:
            info["País"] = ""  # O establece una cadena vacía
    else:
        info["País"] = ""  # O establece una cadena vacía

    issn_match = patrones["ISSN"].search(texto)
    if issn_match:
        info["ISSN"] = issn_match.group(1)

    año_match = patrones["Año"].search(texto)
    if año_match:
        info["Año"] = año_match.group(1)

    volumen_match = patrones["Volumen"].search(texto)
    if volumen_match:
        info["Volumen"] = volumen_match.group(1)
    
    fasciculo_match = patrones["Fascículo"].search(texto)
    if fasciculo_match:
        fasciculo = fasciculo_match.group(1).strip()
        # Reemplazar "N/A" o "N. A" por una cadena vacía
        if fasciculo.upper() in ["N/A", "N. A", "NA"]:
            info["Fascículo"] = ""
        else:
            info["Fascículo"] = fasciculo

    paginas_match = patrones["Páginas"].search(texto)
    if paginas_match:
        info["Páginas"] = paginas_match.group(1)

    doi_match = patrones["DOI"].search(texto)
    if doi_match:
        doi = doi_match.group(1).strip()  # Elimina espacios al inicio y al final
        if doi.endswith('_'):
            doi = doi[:-1]  # Elimina el último carácter si es un guion bajo
        info["DOI"] = doi
        
    todo_match = patrones["todo"].search(texto)
    if todo_match:
        info["todo"] = todo_match.group().strip()

    autores_match = patrones["Autores"].search(texto)
    if autores_match:
        autores_raw = autores_match.group(1)
        # Eliminar los guiones bajos y espacios extra
        autores_cleaned = re.sub(r'\s*_\s*', ' ', autores_raw)
        # Dividir los autores y limpiar cada nombre
        info["Autores"] = [autor.strip() for autor in autores_cleaned.split(',')]        
   

    return info



#expresiones regulares para extraer la información de un libro
def extraer_info_libro(texto, tipo_publicacion):
    info = {
        "Tipo": tipo_publicacion,
        "Tipo Publicación":"",
        "Título": "",
        "País": "",
        "Año": "",
        "ISBN": "",
        "Editorial": "",
        "Autores": [],
        "todo":""
    }

    # Compilar las expresiones regulares
    patrones = {
       "Tipo Publicación": re.compile(r"-\s*(.*?):"),
        "Título": re.compile(r"-\s*(?:[^:]+:)?\s*(.*?)\s+_"),
        "País": re.compile(r"_[^_]*_\s*([^,]+?)\s*,", re.IGNORECASE),
        "Año": re.compile(r"_\s*(\d{4})\s*,"),
        "ISBN": re.compile(r"ISBN:\s*([\d-]+)", re.IGNORECASE),
        "Editorial": re.compile(r"(?:Editorial:|Ed\.)\s*(.+?)(?=Autores:)", re.IGNORECASE),
        "Autores": re.compile(r"Autores:\s*(.+)$", re.IGNORECASE),
        "todo": re.compile(r"-.*,(?=[^,]*$)")
    }
    todo_match = patrones["todo"].search(texto)
    if todo_match:
        info["todo"] = todo_match.group().strip()
        
    año_match = patrones["Año"].search(texto)
    if año_match:
        info["Año"] = año_match.group(1)
    # Extraer información usando las expresiones regulares compiladas
    for key, patron in patrones.items():
        match = patron.search(texto)
        if match:
            if key == "Autores":
                autores_raw = match.group(1)
                autores_cleaned = re.sub(r'\s*_\s*', ' ', autores_raw)
                info[key] = [autor.strip() for autor in autores_cleaned.split(',')]
            elif key == "Editorial":
                editorial = match.group(1).strip()
                info[key] = editorial[:-1] if editorial.endswith('_') else editorial
            elif match.groups():  # Verificar si hay grupos capturados
                info[key] = match.group(1).strip()
            else:
                info[key] = match.group(0).strip()  

    return info


# expresiones regulares para extraer la información de un capítulo de libro

def extraer_info_capitulo_libro(texto, tipo_publicacion):
    info = {
        "Tipo": tipo_publicacion,
        "Tipo Publicación":"",
        "Título": "",
        "País": "",
        "Año": "",
        "Título libro": "",
        "ISBN": "",
        "Volumen": "",
        "Páginas": "",
        "Editorial": "",
        "Autores": [],
        "todo":""
    }

    # Compilar las expresiones regulares
    patrones = {
        "Tipo Publicación": re.compile(r"-\s*(.*?):"),
        "Título": re.compile(r"-\s*(?:[^:]+:)?\s*(.*?)\s+_"),
        "País": re.compile(r"_[^_]*_\s*([^,]+?)\s*,", re.IGNORECASE),
        "Año": re.compile(r"(?:Año:|,)\s*(\d{4})", re.IGNORECASE),
        "Título libro": re.compile(r"[^,]*?,\s*[^,]*?,\s*([^,]*?)\s*,", re.IGNORECASE),  
        "ISBN": re.compile(r"ISBN:\s*([\d-]+)", re.IGNORECASE),
        "Volumen": re.compile(r"Vol\.\s*:?\s*(.*?)(?=\s*,|págs:)", re.IGNORECASE),
        "Páginas": re.compile(r"págs:\s*(\d+\s*-\s*\d+)", re.IGNORECASE),
        "Editorial": re.compile(r"(?:Editorial:|Ed\.)\s*(.+?)(?=Autores:)", re.IGNORECASE),
        "Autores": re.compile(r"Autores:\s*(.+)$", re.IGNORECASE),
        "todo": re.compile(r"-.*,(?=[^,]*$)")
    }
    todo_match = patrones["todo"].search(texto)
    if todo_match:
        info["todo"] = todo_match.group().strip()

    # Extraer información usando las expresiones regulares compiladas
    for key, patron in patrones.items():
        match = patron.search(texto)
        if match:
            if key == "Autores":
                autores_raw = match.group(1)
                autores_cleaned = re.sub(r'\s*_\s*', ' ', autores_raw)
                info[key] = [autor.strip() for autor in autores_cleaned.split(',')]
            elif key == "Editorial":
                editorial = match.group(1).strip()
                info[key] = editorial[:-1] if editorial.endswith('_') else editorial
            elif match.groups():  # Verificar si hay grupos capturados
                info[key] = match.group(1).strip()
            else:
                info[key] = match.group(0).strip()  
    return info

#extraccion para los miembos de los grupos
def extraer_miembros_grupo(soup, nombre_grupo):
    miembros = []
    tabla_miembros = None
    
    # Función auxiliar para limpiar el nombre del miembro
    def limpiar_nombre_miembro(nombre):
        # Elimina la numeración, el guion y los espacios al principio
        return re.sub(r'^\d+\.\s*-\s*', '', nombre).strip()
    
    tablas = soup.find_all('table')
    for tabla in tablas:
        primera_fila = tabla.find('tr')
        if primera_fila:
            primera_celda = primera_fila.find('td')
            if primera_celda and "Integrantes del grupo" in primera_celda.get_text(strip=True):
                tabla_miembros = tabla
                break
    
    if tabla_miembros:
        filas = tabla_miembros.find_all('tr')[2:]  # Ignorar la fila del título
        for fila in filas:
            celdas = fila.find_all('td')
            if len(celdas) >= 4:
                nombre_miembro = limpiar_texto(celdas[0].text.strip())
                nombre_miembro = limpiar_nombre_miembro(nombre_miembro)  # Aplicamos la limpieza aquí
                vinculacion = limpiar_texto(celdas[3].text.strip())
                estado = "Activo" if "Actual" in vinculacion else "Inactivo"
                miembros.append({
                    
                    'Nombre del integrante': nombre_miembro,
                    'Estado': estado
                })
    
    return miembros




# Función para obtener y procesar los datos
def obtener_y_procesar_datos():
    try:
        # Eliminar datos existentes de ambas colecciones
        grupos_collection.delete_many({})
        miembros_collection.delete_many({})
        

        response = session.get(url, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        tabla = soup.find('table', {'id': 'grupos'})
        
        if tabla:
            filas = tabla.find_all('tr')[1:]  # Ignorar la primera fila (encabezados)
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                resultados = list(executor.map(procesar_grupo, filas))
            
            resultados = [r for r in resultados if r is not None]
            
            # Obtener información adicional para cada grupo
            with ThreadPoolExecutor(max_workers=10) as executor:
                info_adicional = list(executor.map(info_grupo_publicaciones, [r['enlace_gruplac'] for r in resultados]))
            
            # Combinar la información original con la información adicional
            for i, resultado in enumerate(resultados):
                resultado.update(info_adicional[i])
            
            # Recolectar los documentos para MongoDB
            grupos_documentos = []
            miembros_documentos = []
        

            for resultado in resultados:
                grupo_info = {
                    'nombre_grupo': resultado.get('titulo', ''),
                    'año_mes_formacion': resultado.get('Año y mes de formación', ''),
                    'departamento': resultado.get('Departamento', ''),
                    'ciudad': resultado.get('Ciudad', ''),
                    'lider': resultado.get('Líder', ''),
                    'pagina_web': resultado.get('Página web', ''),
                    'email': resultado.get('E-mail', ''),
                    'clasificacion': resultado.get('Clasificación', '')[:1] if resultado.get('Clasificación') else '',
                    'area_conocimiento': resultado.get('Área de conocimiento', ''),
                    'programa_ciencia_tecnologia': resultado.get('Programa nacional de ciencia y tecnología', ''),
                    'programa_ciencia_tecnologia_secundario': resultado.get('Programa nacional de ciencia y tecnología (secundario)', ''),
                    'plan_estrategico': resultado.get("Plan Estratégico", ""),
                    'lineas_investigacion': resultado.get("Líneas de investigación", []),
                    'miembros': resultado.get('miembros', []) ,
                    'publicaciones': []
                }

                tipos_publicaciones = [
                    'Artículos publicados', 
                    'Otros artículos publicados',
                    'Libros publicados', 
                    'Capítulos de libro publicados'
                ]
                
                for tipo_base in tipos_publicaciones:
                    for avalado in [True, False]:
                        tipo_publicacion = tipo_base if avalado else f"{tipo_base} sin chulo"
                        for publicacion in resultado.get(tipo_publicacion, []):
                          
                            if tipo_base in ['Artículos publicados', 'Otros artículos publicados']:
                                info_publicacion = extraer_info_articulo("; ".join(publicacion), tipo_base)
                            elif tipo_base == 'Libros publicados':
                                info_publicacion = extraer_info_libro("; ".join(publicacion), tipo_base)
                            elif tipo_base == 'Capítulos de libro publicados':
                                info_publicacion = extraer_info_capitulo_libro("; ".join(publicacion), tipo_base)
                            else:
                                # Para otros tipos de publicaciones, guardar la información sin procesar
                                info_publicacion = {
                                    'tipo': tipo_base,
                                    'contenido': "; ".join(publicacion),
                                }
                            info_publicacion['avalado'] = avalado
                            grupo_info['publicaciones'].append(info_publicacion)

                grupos_documentos.append(grupo_info)

                for miembro in resultado.get('miembros', []):
                    miembro_info = {
                        'nombre_grupo': grupo_info['nombre_grupo'],
                        'nombre_integrante': miembro['Nombre del integrante'],
                        'estado': miembro['Estado']
                    }
                    miembros_documentos.append(miembro_info)

            # Insertar todos los documentos en MongoDB
            if grupos_documentos:
                grupos_collection.insert_many(grupos_documentos)

          

            print(f"Se han guardado {grupos_collection.count_documents({})} grupos en MongoDB")
            print(f"Se han guardado {miembros_collection.count_documents({})} miembros en MongoDB")
    
    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")



# Ejecutar la función principal
def actualizar_base_datos():
    """
    Función principal para actualizar la base de datos.
    Esta función será llamada por el Cron Job de Render.
    """
    print(f"Iniciando actualización de la base de datos: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        obtener_y_procesar_datos()
        print(f"Actualización completada con éxito: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return True
    except Exception as e:
        print(f"Error durante la actualización: {e}")
        return False

# Si el script se ejecuta directamente, ejecutar la actualización
if __name__ == "__main__":
    actualizar_base_datos()