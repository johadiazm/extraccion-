import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry 
import urllib3
from bs4 import BeautifulSoup, SoupStrainer
import os
from concurrent.futures import ThreadPoolExecutor
import re
import spacy
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


from openpyxl.utils import get_column_letter


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
    status_forcelist=[429, 500, 502, 503, 504],
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


def extraer_contenido_tabla(tabla):
    contenido = []
    filas = tabla.find_all('tr')[1:]  # Ignorar la fila del título
    for fila in filas:
        celdas = fila.find_all('td')
        if celdas:
            contenido.append(limpiar_texto(celdas[0].text.strip()))
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
                    "año y mes de formacion",
                    "Departamento - ciudad",
                    "Líder",
                    "Informacion certificada",
                    "Pagina Web",
                    "Email",
                    "Clasificacion",
                    "Area de conocimiento",
                    "Programa nacional de ciencia y tecnología",
                    "Programa nacional de ciencia y tecnología (secundario)"
                ]

                for i, campo in enumerate(campos, start=1):
                    if i < len(filas):
                        celdas = filas[i].find_all('td')
                        if len(celdas) >= 2:
                            valor = celdas[1].text.strip().replace('\xa0', ' ').replace('\r\n', ' ')
                            grupo[campo] = valor


        # Buscar las tablas específicas
        tablas = soup.find_all('table')
        for tabla in tablas:
            titulo = tabla.find('td', class_='celdaEncabezado')
            if titulo:
                if "Plan Estratégico" in titulo.text:
                    grupo["Plan Estratégico"] = extraer_contenido_tabla(tabla)
                elif "Líneas de investigación declaradas por el grupo" in titulo.text:
                    grupo["Líneas de investigación"] = extraer_contenido_tabla(tabla)
  

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
    texto = re.sub(r'\s+', ' ', texto)
    # Elimina espacios antes de comas y puntos
    texto = re.sub(r'\s+([,.])', r'\1', texto)
    # Asegura un espacio después de comas y puntos
    texto = re.sub(r'([,.])\s*', r'\1 ', texto)
    # Elimina punto y coma al inicio si existe
    texto = texto.lstrip(';')
    return texto.strip()



def extraer_info_articulo(texto, tipo_publicacion):
    info = {
        "tipo": tipo_publicacion,
        "titulo": "",
        "revista": "",
        "pais": "",
        "issn": "",
        "año": "",
        "volumen": "",
        "fascículo": "",
        "paginas": "",
        "doi": "",
        "autores": []
    }

    # Compilar las expresiones regulares para mejorar el rendimiento
    patrones = {
        "titulo": re.compile(r"Publicado en revista especializada:\s*(.*?)(?=<br>|$)"),
        "revista_pais": re.compile(r"<br>\s*(.*?),\s*(.*?)\s*ISSN:"),
        "issn": re.compile(r"ISSN:\s*(\d{4}-\d{3}[\dX])"),
        "año": re.compile(r"\b(\d{4})\b"),
        "volumen": re.compile(r"vol:(\d+)"),
        "fasciculo": re.compile(r"fasc:\s*(N/A|\d+)"),
        "paginas": re.compile(r"págs:\s*(\d+\s*-\s*\d+)"),
        "doi": re.compile(r"DOI:\s*([\w\./-]+)"),
        "autores": re.compile(r"Autores:\s*(.+)$")
    }

    # Extraer información usando las expresiones regulares compiladas
    titulo_match = patrones["titulo"].search(texto)
    if titulo_match:
        info["titulo"] = titulo_match.group(1).strip()

    revista_pais_match = patrones["revista_pais"].search(texto)
    if revista_pais_match:
        info["pais"] = revista_pais_match.group(1).strip()
        info["revista"] = revista_pais_match.group(2).strip()

    issn_match = patrones["issn"].search(texto)
    if issn_match:
        info["issn"] = issn_match.group(1)

    año_match = patrones["año"].search(texto)
    if año_match:
        info["año"] = año_match.group(1)

    volumen_match = patrones["volumen"].search(texto)
    if volumen_match:
        info["volumen"] = volumen_match.group(1)

    fasciculo_match = patrones["fasciculo"].search(texto)
    if fasciculo_match:
        info["fascículo"] = fasciculo_match.group(1)

    paginas_match = patrones["paginas"].search(texto)
    if paginas_match:
        info["paginas"] = paginas_match.group(1)

    doi_match = patrones["doi"].search(texto)
    if doi_match:
        info["doi"] = doi_match.group(1)

    autores_match = patrones["autores"].search(texto)
    if autores_match:
        info["autores"] = [autor.strip() for autor in autores_match.group(1).split(',')]

    return info

# Ejemplo de uso
texto_articulo = """Publicado en revista especializada Titulo articulo:Synthesis and characterization of natural rubber/clay nanocomposite to develop electrical safety gloves Pais:reino unido ISSN:2214-7853, 2020 Volumen:33 fasc: N/A págs: 1949 - 1953  DOI:10. 1016/j. matpr. 2020. 05. 795 Autores: MARTIN EMILIO MENDOZA OLIVEROS, CARLOS EDUARDO PINTO SALAMANCA"""



#extraccion para los miembos de los grupos
def extraer_miembros_grupo(soup, nombre_grupo):
    miembros = []
    tabla_miembros = None
    
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
                vinculacion = limpiar_texto(celdas[3].text.strip())
                estado = "Activo" if "Actual" in vinculacion else "Inactivo"
                miembros.append({
                    'Nombre del grupo': nombre_grupo,
                    'Nombre del integrante': nombre_miembro,
                    'Estado': estado
                })
    
    return miembros




# Función para obtener y procesar los datos
def obtener_y_procesar_datos():
    try:
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
                    'año_mes_formacion': resultado.get('año y mes de formacion', ''),
                    'departamento_ciudad': resultado.get('Departamento - ciudad', ''),
                    'lider': resultado.get('Líder', ''),
                    'informacion_certificada': resultado.get('Informacion certificada', ''),
                    'pagina_web': resultado.get('Pagina Web', ''),
                    'email': resultado.get('Email', ''),
                    'clasificacion': resultado.get('Clasificacion', ''),
                    'area_conocimiento': resultado.get('Area de conocimiento', ''),
                    'programa_ciencia_tecnologia': resultado.get('Programa nacional de ciencia y tecnología', ''),
                    'programa_ciencia_tecnologia_secundario': resultado.get('Programa nacional de ciencia y tecnología (secundario)', ''),
                    'plan_estrategico': resultado.get("Plan Estratégico", ""),
                    'lineas_investigacion': resultado.get("Líneas de investigación", ""),
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
                            info_publicacion = extraer_info_articulo("; ".join(publicacion), tipo_base)
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

            if miembros_documentos:
                miembros_collection.insert_many(miembros_documentos)

            print(f"Se han guardado {grupos_collection.count_documents({})} grupos en MongoDB")
            print(f"Se han guardado {miembros_collection.count_documents({})} miembros en MongoDB")
    
    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")



# Ejecutar la función principal
obtener_y_procesar_datos()



