import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry 
import urllib3
from bs4 import BeautifulSoup, SoupStrainer
import csv
import os
from concurrent.futures import ThreadPoolExecutor
import re
import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

import openpyxl
from openpyxl.utils import get_column_letter



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
            
            # Crear un nuevo libro de trabajo Excel y seleccionar la hoja activa
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Resultados Grupos"

            # Escribir los encabezados
            headers = ['Nombre Grupo', 
                    'Año y Mes de Formación', 'Departamento - Ciudad', 'Líder',
                    'Información Certificada', 'Página Web', 'Email', 'Clasificación', 
                    'Área de Conocimiento', 'Programa Nacional de Ciencia y Tecnología', 
                    'Programa Nacional de Ciencia y Tecnología (Secundario)',
                    'Plan Estratégico', 'Líneas de Investigación', 
                    'Artículos Publicados', 'Artículos Publicados sin chulo',
                    'Otros Artículos Publicados', 'Otros Artículos Publicados sin chulo',
                    'Libros publicados', 'Libros publicados sin chulo',
                    'Capítulos de libro publicados', 'Capítulos de libro publicados sin chulo']
            for col, header in enumerate(headers, start=1):
                ws.cell(row=1, column=col, value=header)

            # Escribir los datos
            for row, resultado in enumerate(resultados, start=2):
                ws.cell(row=row, column=1, value=resultado.get('titulo', ''))
                ws.cell(row=row, column=2, value=resultado.get('año y mes de formacion', ''))
                ws.cell(row=row, column=3, value=resultado.get('Departamento - ciudad', ''))
                ws.cell(row=row, column=4, value=resultado.get('Líder', ''))
                ws.cell(row=row, column=5, value=resultado.get('Informacion certificada', ''))
                ws.cell(row=row, column=6, value=resultado.get('Pagina Web', ''))
                ws.cell(row=row, column=7, value=resultado.get('Email', ''))
                ws.cell(row=row, column=8, value=resultado.get('Clasificacion', ''))
                ws.cell(row=row, column=9, value=resultado.get('Area de conocimiento', ''))
                ws.cell(row=row, column=10, value=resultado.get('Programa nacional de ciencia y tecnología', ''))
                ws.cell(row=row, column=11, value=resultado.get('Programa nacional de ciencia y tecnología (secundario)', ''))
                ws.cell(row=row, column=12, value=resultado.get("Plan Estratégico", ""))
                ws.cell(row=row, column=13, value=resultado.get("Líneas de investigación", ""))                

                # Escribir los artículos publicados en una sola celda
                articulos_publicados = "\n\n".join(["; ".join(articulo) for articulo in resultado.get("Artículos publicados", [])])
                articulos_publicados_sin_chulo = "\n\n".join(["; ".join(articulo) for articulo in resultado.get("Artículos publicados sin chulo", [])])
                otros_articulos_publicados = "\n\n".join(["; ".join(articulo) for articulo in resultado.get("Otros artículos publicados", [])])
                otros_articulos_publicados_sin_chulo = "\n\n".join(["; ".join(articulo) for articulo in resultado.get("Otros artículos publicados sin chulo", [])])
                libros_publicados = "\n\n".join(["; ".join(articulo) for articulo in resultado.get("Libros publicados", [])])
                libros_publicados_sin_chulo = "\n\n".join(["; ".join(articulo) for articulo in resultado.get("Libros publicados sin chulo", [])])
                capitulos_libros_publicados = "\n\n".join(["; ".join(articulo) for articulo in resultado.get("Capítulos de libro publicados", [])])
                capitulos_libros_publicados_sin_chulo = "\n\n".join(["; ".join(articulo) for articulo in resultado.get("Capítulos de libro publicados sin chulo", [])])

                ws.cell(row=row, column=14, value=articulos_publicados)
                ws.cell(row=row, column=15, value=articulos_publicados_sin_chulo)
                ws.cell(row=row, column=16, value=otros_articulos_publicados)
                ws.cell(row=row, column=17, value=otros_articulos_publicados_sin_chulo)
                ws.cell(row=row, column=18, value=libros_publicados)
                ws.cell(row=row, column=19, value=libros_publicados_sin_chulo)
                ws.cell(row=row, column=20, value=capitulos_libros_publicados)
                ws.cell(row=row, column=21, value=capitulos_libros_publicados_sin_chulo)

            # Ajustar el ancho de las columnas
            for col in range(1, 17):
                ws.column_dimensions[get_column_letter(col)].auto_size = True

            # Guardar el archivo Excel
            wb.save(archivo_salida_excel)
            
            print(f"Se han guardado {len(resultados)} resultados en {archivo_salida_excel}")
        else:
            print("No se encontró la tabla de grupos en la página.")
    
    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")


# Ejecutar la función principal
obtener_y_procesar_datos()