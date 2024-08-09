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
                'nombre_grupo': nombre_grupo,
                'enlace_gruplac': enlace_gruplac_grupo,
                'nombre_lider': nombre_lider
             
            }

    return None

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
                    "Lider",
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

        # Obtener las tablas de "Artículos publicados" y "Otros artículos publicados"
        tablas_validas = []
        titulos_validos = ["Artículos publicados", "Otros artículos publicados"]
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
                    if titulo_tabla not in grupo:
                        grupo[titulo_tabla] = []
                    for fila in filas_articulos:
                         if fila.find('img'):
                            celdas_fila = [limpiar_texto(celda.text) for celda in fila.find_all('td')]
                            grupo[titulo_tabla].append(celdas_fila)

    except requests.exceptions.RequestException as e:
        print(f"Error al obtener información del grupo: {e}")

    return grupo

# Función para limpiar texto de caracteres no válidos para Excel
def limpiar_texto(texto):
    # Elimina caracteres no imprimibles usando una expresión regular
    return re.sub(r'[\x00-\x1F\x7F-\x9F]', '', texto)

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
            headers = ['Nombre Grupo', 'Enlace GrupLAC', 'Nombre Líder', 'Título', 
                       'Año y Mes de Formación', 'Departamento - Ciudad', 'Líder', 
                       'Información Certificada', 'Página Web', 'Email', 'Clasificación', 
                       'Área de Conocimiento', 'Programa Nacional de Ciencia y Tecnología', 
                       'Programa Nacional de Ciencia y Tecnología (Secundario)', 'Artículos Publicados', 'Otros Artículos Publicados']
            for col, header in enumerate(headers, start=1):
                ws.cell(row=1, column=col, value=header)

            # Escribir los datos
            for row, resultado in enumerate(resultados, start=2):
                ws.cell(row=row, column=1, value=resultado['nombre_grupo'])
                ws.cell(row=row, column=2, value=resultado['enlace_gruplac'])
                ws.cell(row=row, column=3, value=resultado['nombre_lider'])
                ws.cell(row=row, column=4, value=resultado.get('titulo', ''))
                ws.cell(row=row, column=5, value=resultado.get('año y mes de formacion', ''))
                ws.cell(row=row, column=6, value=resultado.get('Departamento - ciudad', ''))
                ws.cell(row=row, column=7, value=resultado.get('Lider', ''))
                ws.cell(row=row, column=8, value=resultado.get('Informacion certificada', ''))
                ws.cell(row=row, column=9, value=resultado.get('Pagina Web', ''))
                ws.cell(row=row, column=10, value=resultado.get('Email', ''))
                ws.cell(row=row, column=11, value=resultado.get('Clasificacion', ''))
                ws.cell(row=row, column=12, value=resultado.get('Area de conocimiento', ''))
                ws.cell(row=row, column=13, value=resultado.get('Programa nacional de ciencia y tecnología', ''))
                ws.cell(row=row, column=14, value=resultado.get('Programa nacional de ciencia y tecnología (secundario)', ''))

                # Escribir los artículos publicados en una sola celda
                articulos_publicados = " | ".join(["; ".join(articulo) for articulo in resultado.get("Artículos publicados", [])])
                otros_articulos_publicados = " | ".join(["; ".join(articulo) for articulo in resultado.get("Otros artículos publicados", [])])

                ws.cell(row=row, column=15, value=articulos_publicados)
                ws.cell(row=row, column=16, value=otros_articulos_publicados)

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