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
archivo_salida_csv = 'resultados_grupos_csv.csv'

#Conexion con mongodb
'''
MONGO_URI = "mongodb+srv://jazminasaleh:IRdNyaCqKVvHyhZ3@gruposinvestigacion.gpd7xka.mongodb.net/"

try:
    client = MongoClient(MONGO_URI)
    db = client.grupos_investigacion  # Nombre de la base de datos
    collection = db.grupos  # Nombre de la colección
    print("Conexión a MongoDB Atlas establecida con éxito")
except ConnectionFailure:
    print("No se pudo conectar a MongoDB Atlas")
'''

def cargar_paises_espanol():
    paises = []
    with open('paises_espanol.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            paises.append(row[0])
    return paises

paises_espanol = cargar_paises_espanol()

# Función para extrear la informcaión de los grupos y sus investigadores
def procesar_grupo(fila):
    columnas = fila.find_all('td')

    # Verificar si hay mas de tres columnas en la fila
    if len(columnas) >= 3:
        tercer_td = columnas[2]
        #Se obtiene el enlcae del Gruplac
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
            #Solo deja la primera letra en mayuscula
            nombre_lider = nombre_lider.title()
            cvlac_lider = ''
            enlace_lider = columnas[3].find('a')
            if enlace_lider:
                href_enlace_lider = enlace_lider.get('href')
                numero_url_lider = href_enlace_lider.split('=')[-1]
                cvlac_lider = f'https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh={numero_url_lider}'

            # Lista para almacenar los datos de los integrantes
            integrantes = []

            try:
                response_grupo = session.get(enlace_gruplac_grupo)
                response_grupo.raise_for_status()
                soup_grupo = BeautifulSoup(response_grupo.text, 'html.parser', from_encoding='utf-8')

                tables = soup_grupo.find_all('table')

                for table in tables:
                    primer_tr = table.find('tr')
                    primer_td = primer_tr.find('td')
                    if primer_td and primer_td.text.strip() == "Integrantes del grupo":
                        filas_tabla = table.find_all('tr')[2:]
                        
                        # Obtener nombre de cada investigador, enlace de su CvLac
                        for tercer_tr in filas_tabla:
                            enlaces_integrantes = tercer_tr.find_all('a')
                            for enlace_integrante in enlaces_integrantes:
                                nombre_integrante = enlace_integrante.text.strip()
                                #Solo deja la primera letra en mayuscula
                                nombre_integrante = nombre_integrante.title()
                                enlace_cvlac_integrante = enlace_integrante.get('href')

                                try:
                                    response_cvlac_integrante = session.get(enlace_cvlac_integrante)
                                    response_cvlac_integrante.raise_for_status()
                                    soup_cvlac_integrante = BeautifulSoup(response_cvlac_integrante.text,  'html.parser', from_encoding='utf-8')

                                    tables_cvlac = soup_cvlac_integrante.find_all('table')

                                    nombre_citaciones = ''
                                    categoria = ''
                                    nacionalidad = ''
                                    sexo = ''
                                    publicaciones= []

                                    # Obtener nombre de cada investigador en citaciones, nacionalidad, sexo y categoría
                                    for table_cvlac in tables_cvlac:
                                        nombre_citaciones_td = table_cvlac.find('td', string='Nombre en citaciones')
                                        if nombre_citaciones_td:
                                            nombre_citaciones = nombre_citaciones_td.find_next('td').text.strip()
                                        nacionalidad_td = table_cvlac.find('td', string='Nacionalidad')
                                        if nacionalidad_td:
                                            nacionalidad = nacionalidad_td.find_next('td').text.strip()

                                        sexo_td = table_cvlac.find('td', string='Sexo')
                                        if sexo_td:
                                            sexo = sexo_td.find_next('td').text.strip()

                                        categoria_td = table_cvlac.find('td', string='Categoría')
                                        if categoria_td:
                                            categoria = categoria_td.find_next('td').text.strip()
                                            categoria = ' '.join(categoria.split()) #quitar los espacios adicionales
                                        
                                        # Buscar la sección de 'Artículos','Libros','Capitulos de libro', 'Textos en publicaciones no científicas'
                                        seccion_publicacion = table_cvlac.find('h3', string=['Artículos','Libros','Capitulos de libro', 'Textos en publicaciones no científicas'])
                                        if seccion_publicacion:
                                            tipo_producto = seccion_publicacion.text
                                            # Encontrar la fila (tr) siguiente después de la sección "Artículos"
                                            fila_publicacion = seccion_publicacion.find_parent('tr').find_next_sibling('tr')
                                           
                                            
                                            # Extraer los artículos de la segunda fila (tr)
                                            while fila_publicacion:
                                                celdas_publicacion = fila_publicacion.find_all('td')
                                               
                                                
                                                #Dentro de blockquote es donde se encuentra toda la informacion de las publicaciones 
                                                if celdas_publicacion:
                                                    elementos_blockquote = celdas_publicacion[0].find('blockquote')
                                                    elementos_li = celdas_publicacion[0].find_all('li')

                                                     # Obtener estado y tipo de publicacion
                                                    if elementos_li:
                                                        for elemento in elementos_li:
                                                            texto_publicacion=elemento.find('b')
                                                            tipo_publicacion = ''
                                                            if texto_publicacion:
                                                                tipo_publicacion = texto_publicacion.get_text().split(' - ')[-1]
                                                            else:
                                                                tipo_publicacion = "Capítulo de Libro"
                                                            img_tag = elemento.find('img')
                                                            estado = 'Vigente' if img_tag else 'No Vigente'
                                                    #Obtener los datos de la publicación
                                                    titulo_revista = ""
                                                    if elementos_blockquote:
                                                        texto_blockquote = elementos_blockquote.get_text(strip=True)
                                                        texto_blockquote = " ".join(texto_blockquote.split()) 
                                                       
                                                        indice_comilla1 = texto_blockquote.find('"')
                                                        if indice_comilla1 != -1:
                                                            indice_comilla2 = texto_blockquote.find('"', indice_comilla1 + 1)
                                                            #Obtener titulo de la publicación
                                                            if indice_comilla2 != -1:
                                                                titulo_publicacion = texto_blockquote[indice_comilla1 + 1:indice_comilla2]
                                                                tipo_publicacion = tipo_publicacion.title()
                                                                indice_pais = texto_blockquote.find("En:")
                                                                if indice_pais != -1:
                                                                    indice_palabra_despues_de_en = indice_pais + len("En:")
                                                                   
                                                                    palabras = []
                                                                    palabra_actual = ""
                                                                    for i in range(indice_palabra_despues_de_en, len(texto_blockquote)):
                                                                        if texto_blockquote[i].isalnum() or (texto_blockquote[i] == "." and palabra_actual):
                                                                            palabra_actual += texto_blockquote[i]
                                                                        elif texto_blockquote[i] in [","]:
                                                                            palabras.append(palabra_actual)
                                                                            palabra_actual = ""
                                                                            if len(palabras) == 3:
                                                                                break
                                                                        elif texto_blockquote[i] == " ":
                                                                            if palabra_actual:
                                                                                palabras.append(palabra_actual)
                                                                                palabra_actual = ""
                                                                            else:
                                                                                continue
                                                                        else:
                                                                            break
                                                                    
                                                                    #Obtener el pais
                                                                    paises_validos = [pais.lower() for pais in paises_espanol]
                                                                    pais_encontrado = False
                                                                    titulo_revista = ""
                                                                    for pais_valido in paises_validos:
                                                                        nombre_pais = " ".join(palabras[:3]).lower()
                                                                        
                                                                        if nombre_pais == pais_valido or nombre_pais.startswith(pais_valido + " ") or nombre_pais.startswith(pais_valido):
                                                                            pais = pais_valido.capitalize()
                                                                           
                                                                            pais_encontrado = True
                                                                            break
                                                                    if not pais_encontrado:
                                                                        pais = ""
                                                                    
                                                                    #Obtener el titulo de la revista donde se público el articulo o Textos en publicaciones no científicas y el pais en caso que no se haya obtennido antes
                                                                    if tipo_producto == "Artículos":
                                                                        # Suponiendo que 'pais' y 'texto_blockquote' ya están definidos anteriormente
                                                                        if len(pais) == 0:
                                                                            indice_final_pais = texto_blockquote.find("En:") + 3
                                                                            indice_issn = texto_blockquote.find("ISSN:")
                                                                            
                                                                            # Verificamos que se han encontrado las posiciones
                                                                            if indice_final_pais != -1 and indice_issn != -1:
                                                                                texto_despues_pais = texto_blockquote[indice_final_pais:indice_issn]
                                                                                texto_despues_pais = texto_despues_pais.lstrip()  # Eliminar espacios al principio
                                                                                
                                                                                # Convertimos a minúsculas los nombres de los países válidos
                                                                                paises_validos = [p.lower() for p in paises_espanol]
                                                                                pais_encontrado = False
                                                                                
                                                                                for pais_valido in paises_validos:
                                                                                    # Convertimos el texto a minúsculas para comparación
                                                                                    nombre_pais = texto_despues_pais.lower()
                                                                                    
                                                                                    # Verificamos si el texto coincide con algún país válido
                                                                                    if nombre_pais == pais_valido or nombre_pais.startswith(pais_valido + " ") or nombre_pais.startswith(pais_valido):
                                                                                        pais = pais_valido.capitalize()  # Capitalizamos el nombre del país encontrado
                                                                                        pais_encontrado = True
                                                                                        break
                                                                                
                                                                                if not pais_encontrado:
                                                                                    pais = ""
                                                                            else:
                                                                                pais = ""

                                                                                
                                                                            if len(texto_despues_pais) > 1:
                                                                                titulo_revista = texto_despues_pais.replace(pais, "").strip()
                                                                                if titulo_revista.startswith("No Aplica"):
                                                                                    titulo_revista = titulo_revista[9:].strip()
                                                                            else:
                                                                                titulo_revista = ""
                                                                        else:
                                                                            indice_final_pais = texto_blockquote.find("En:") + len(pais) + 4
                                                                            indice_issn = texto_blockquote.find("ISSN:")
                                                                            texto_despues_pais = texto_blockquote[indice_final_pais:indice_issn]
                                                                            if len(texto_despues_pais) > 1:
                                                                                titulo_revista = texto_despues_pais.replace(pais, "").strip()
                                                                            else:
                                                                                titulo_revista = ""
                                                                                                                                                    
                                                                    elif  tipo_producto == "Textos en publicaciones no científicas":
                                                                        indice_final_pais = texto_blockquote.find("En:") + len(pais) + 12
                                                                        indice_issn = texto_blockquote.find("ISSN:") -1
                                                                        if indice_issn != -1:
                                                                            titulo_revista = texto_blockquote[indice_final_pais:indice_issn].strip()
                                                                        else:
                                                                            titulo_revista = ""
                                                                    else:
                                                                        titulo_revista = ""
                                                                    
                                                                issn = ''

                                                                issn = obtener_issn(texto_blockquote)  
                                                                editorial = obtener_editorial(texto_blockquote)
                                                                volumen = obtener_volumen(texto_blockquote)
                                                                fasciculo = obtener_fasciculo(texto_blockquote)
                                                                paginas = obtener_paginas(texto_blockquote)
                                                                año = obtener_año(texto_blockquote)
                                                                doi = obtener_doi(texto_blockquote)
                                                                palabras = obtener_palabras_clave(texto_blockquote)
                                                                areas = obtener_areas(texto_blockquote)
                                                                sectores = obtener_sectores(texto_blockquote)
                                                                nombres_integrantes_str = obtener_integrantes(texto_blockquote,  indice_comilla1)
                                                                nombres_integrantes_lista = nombres_integrantes_str.split(',')
                                                                
                                                                publicacion_existente = next((a for a in publicaciones if a[0] == titulo_publicacion), None)
                                                                                                                              
                                                                if publicacion_existente is not None:
                                                                    if isinstance(publicacion_existente[1], list):
                                                                        publicacion_existente[1].extend([nombre for nombre in nombres_integrantes_lista if nombre.strip() and nombre.strip() not in publicacion_existente[1]])
                                                                    else:
                                                                        # Si publicacion_existente[1] no es una lista, puedes crear una nueva lista con los nombres anteriores y los nuevos
                                                                        publicacion_existente = list(publicacion_existente)
                                                                        publicacion_existente[1] = publicacion_existente[1].split(', ') + [nombre for nombre in nombres_integrantes_lista if nombre.strip() and nombre.strip() not in publicacion_existente[1]]
                                                                else:
                                                                    # Si el artículo no está en la lista, agregarlo
                                                                   publicaciones.append((titulo_publicacion, nombres_integrantes_str,  tipo_producto, tipo_publicacion, estado, pais, titulo_revista, issn, editorial, volumen, fasciculo, paginas, año, doi, palabras, areas, sectores))
                                                                   
                                                #Va a la siguinete publicacion               
                                                fila_publicacion = fila_publicacion.find_next_sibling('tr')
                                               
                                    # Agregar los datos del integrante y sus artículos a la lista
                                    integrantes.append([nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria,publicaciones])

                                except requests.exceptions.RequestException:
                                    # En caso de error en la solicitud HTTP
                                    integrantes.append(['', '', '', '', '', []])

            except requests.exceptions.RequestException:
                # En caso de error en la solicitud HTTP
                integrantes = []

            # Devolver los datos del grupo y sus integrantes
            return [nombre_grupo, enlace_gruplac_grupo, nombre_lider, cvlac_lider, integrantes]

    return []

#Obtener ISSN del articulo o textos en publicaciones no cinetificas
def obtener_issn(texto_blockquote):
    indice_issn = texto_blockquote.find("ISSN:")
    if indice_issn != -1:
        indice_ed = texto_blockquote.find("ed", indice_issn)
        indice_p = texto_blockquote.find("p.", indice_issn)
        if indice_ed != -1 and indice_p != -1:
            if indice_ed < indice_p:
                return texto_blockquote[indice_issn + len("ISSN:"):indice_ed].strip()
            else:
                return texto_blockquote[indice_issn + len("ISSN:"):indice_p].strip()
        elif indice_ed != -1:
            return texto_blockquote[indice_issn + len("ISSN:"):indice_ed].strip()
        elif indice_p != -1:
            return texto_blockquote[indice_issn + len("ISSN:"):indice_p].strip()
        else:
            return texto_blockquote[indice_issn + len("ISSN:"):].strip()   
    return ""

def obtener_editorial(texto_blockquote):
    indice_ed = texto_blockquote.find("ed:", texto_blockquote.find("ISSN:"))
    if indice_ed != -1:
        indice_v = texto_blockquote.find("v.", indice_ed)
        if indice_v != -1:
            return texto_blockquote[indice_ed + len("ed:"):indice_v].strip()
        else:
            return texto_blockquote[indice_ed + len("ed:"):].strip()
    return ""

def obtener_volumen(texto_blockquote):
    indice_v = texto_blockquote.find("v.", texto_blockquote.find("ISSN:"))
    if indice_v != -1:
        indice_fasc = texto_blockquote.find("fasc.", indice_v)
        indice_palabras_clave = texto_blockquote.find("Palabras:", indice_v)
        if indice_fasc != -1:
            volumen = texto_blockquote[indice_v + len("v."):indice_fasc].strip()
        elif indice_palabras_clave != -1:
            volumen = texto_blockquote[indice_v + len("v."):indice_palabras_clave].strip()
        else:
            volumen = texto_blockquote[indice_v + len("v."):].strip()
        return volumen if volumen.isdigit() else ""
    return ""

def obtener_fasciculo(texto_blockquote):
    indice_fasc = texto_blockquote.find("fasc.", texto_blockquote.find("v."))
    if indice_fasc != -1:
        indice_p = texto_blockquote.find("p.", indice_fasc)
        if indice_p != -1:
            fasciculo = texto_blockquote[indice_fasc + len("fasc."):indice_p].strip()
        else:
            fasciculo = texto_blockquote[indice_fasc + len("fasc."):].strip()
        return "" if fasciculo.isdigit() and len(fasciculo) == 4 else fasciculo
    return ""

# Obtener el número de pagina
def obtener_paginas(texto_blockquote):
    patron_pagina = r'(?:pages?|p\.)\s*(\d+)\s*-\s*(\d+)'
    resultado_pagina = re.search(patron_pagina, texto_blockquote)
    if resultado_pagina:
        return f"{resultado_pagina.group(1)}-{resultado_pagina.group(2)}"
    return ""

 # Obtener año de publicación
def obtener_año(texto_blockquote):
    indices_comas = [m.start() for m in re.finditer(r',', texto_blockquote)]
    indices_puntos = [m.start() for m in re.finditer(r'. ', texto_blockquote)]
    
    for i in range(len(indices_comas) - 1):
        posible_año = texto_blockquote[indices_comas[i] + 1:indices_comas[i + 1]].strip()
        if posible_año.isdigit() and len(posible_año) == 4:
            return posible_año
    
    for indice_punto in indices_puntos:
        posible_año = texto_blockquote[indice_punto + 2:indice_punto + 6]
        if posible_año.isdigit():
            return posible_año
    
    return ""

def obtener_doi(texto_blockquote):
    indice_doi = texto_blockquote.find("DOI:")
    if indice_doi != -1:
        indice_palabras = texto_blockquote.find("Palabras:", indice_doi)
        indice_sectores = texto_blockquote.find("Sectores:", indice_doi)
        indice_doi_dos = texto_blockquote.find("doi:", indice_doi)
        if indice_palabras != -1:
            return texto_blockquote[indice_doi + len("DOI:"):indice_palabras].strip()
        elif indice_sectores != -1:
            return texto_blockquote[indice_doi + len("DOI:"):indice_sectores].strip()
        elif indice_doi_dos != -1:
            return texto_blockquote[indice_doi_dos + len("doi:"):].strip()
        else:
            return texto_blockquote[indice_doi + len("DOI:"):].strip()
    return ""

#Obtener palabras claves de la publicacion
def obtener_palabras_clave(texto_blockquote):
    indice_palabras = texto_blockquote.find("Palabras:")
    if indice_palabras != -1:
        indice_sectores = texto_blockquote.find("Sectores:", indice_palabras)
        if indice_sectores != -1:
            palabras_texto = texto_blockquote[indice_palabras + len("Palabras:"):indice_sectores].strip()
        else:
            palabras_texto = texto_blockquote[indice_palabras + len("Palabras:"):].strip()
        palabras_limpio = [palabra.strip() for palabra in palabras_texto.split(",") if palabra.strip()]
        return ', '.join(palabras_limpio)
    return ""

def obtener_areas(texto_blockquote):
    indice_areas = texto_blockquote.find("Areas:")
    if indice_areas != -1:
        indice_sectores = texto_blockquote.find("Sectores:", indice_areas)
        if indice_sectores != -1:
            areas_texto = texto_blockquote[indice_areas + len("Areas:"):indice_sectores].strip()
        else:
            areas_texto = texto_blockquote[indice_areas + len("Areas:"):].strip()
        areas_limpio = [area.strip() for area in areas_texto.split(",") if area.strip()]
        return ', '.join(areas_limpio)
    return ""

def obtener_sectores(texto_blockquote):
    indice_sectores = texto_blockquote.find("Sectores:")
    if indice_sectores != -1:
        sectores_texto = texto_blockquote[indice_sectores + len("Sectores:"):].strip()
        sectores_limpio = [sector.strip() for sector in sectores_texto.split(",") if sector.strip()]
        return ', '.join(sectores_limpio)
    return ""

def obtener_integrantes(texto_blockquote, indice_comilla1):
    nombres_integrantes = texto_blockquote[:indice_comilla1].split(',')
    nombres_integrantes_limpios = []
    for nombre in nombres_integrantes:
        indice_tipo_capitulo = nombre.find("Tipo: Capítulo de libro")
        indice_tipo_otro_capitulo = nombre.find("Tipo: Otro capítulo de libro publicado")
        
        if indice_tipo_capitulo != -1:
            indice_tipo = indice_tipo_capitulo
        elif indice_tipo_otro_capitulo != -1:
            indice_tipo = indice_tipo_otro_capitulo
        else:
            nombre_limpio = re.sub(r"['\\]", '', nombre.strip())
            if nombre_limpio:
                nombres_integrantes_limpios.append(nombre_limpio.title())
            continue
        
        nombre_limpio = re.sub(r"['\\]", '', nombre[:indice_tipo].strip())
        nombres_integrantes_limpios.append(nombre_limpio.title())
    
    return ', '.join(nombres_integrantes_limpios)

try:
    # Realizar la solicitud HTTP
    response = session.get(url, verify=False)
    # Analizar el HTML
    soup = BeautifulSoup(response.text, 'html.parser', from_encoding='utf-8')
    # Encontrar y extraer la información deseada
    filas = soup.find_all('tr')[1:] # Omitir la primera fila que contiene la línea no deseada

    # Abrir el archivo de salida CSV en modo de escritura
    with open(archivo_salida_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        data_json = []

        # El nombre de las columnas en el CSV
        writer.writerow(['Nombre del grupo', 'Enlace al GrupLac', 'Nombre del líder', 'Enlace al CvLac líder',
                         'Nombre del integrante', 'Enlace al CvLac del investigador', 'Nombre en citaciones',
                         'Nacionalidad', 'Sexo', 'Categoría', 'Título publicación', 'Integrantes involucrados',
                         'Tipo producto', 'Tipo publicación', 'Estado', 'País', 'Titulo revista', 'ISSN',
                         'Editorial', 'Volumen', 'Fascículo', 'Páginas', 'Año publicación', 'DOI', 'Palabras clave',
                         'Areas', 'Sectores'])

        # Crear hilos para procesar los grupos
        with ThreadPoolExecutor() as executor:
            # Mapear la función procesar_grupo a cada fila de la tabla
            resultados = list(executor.map(procesar_grupo, filas))

        for datos in resultados:
            if datos:
                grupo, enlace_grupo, lider, cvlac_lider, integrantes = datos
                grupo_data = {
                    'Nombre del grupo': grupo,
                    'Enlace al GrupLac': enlace_grupo,
                    'Nombre del líder': lider,
                    'Enlace al CvLac líder': cvlac_lider,
                    'Integrantes': []
                }
                for integrante in integrantes:
                    if len(integrante) == 7:
                        nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria, publicaciones = integrante
                        integrante_data = {
                            'Nombre del integrante': nombre_integrante,
                            'Enlace al CvLac del investigador': enlace_cvlac_integrante,
                            'Nombre en citaciones': nombre_citaciones,
                            'Nacionalidad': nacionalidad,
                            'Sexo': sexo,
                            'Categoría': categoria,
                            'Publicaciones': []
                        }
                        for publicacion in publicaciones:
                            titulo_publicacion, nombres_integrantes, tipo_producto, tipo_publicacion, estado, pais, titulo_revista, issn, editorial, volumen, fasciculo, paginas, año, doi, palabras, areas, sectores = publicacion
                            publicacion_data = {
                                'Título publicación': titulo_publicacion,
                                'Integrantes involucrados': nombres_integrantes,
                                'Tipo producto': tipo_producto,
                                'Tipo publicación': tipo_publicacion,
                                'Estado': estado,
                                'País': pais,
                                'Titulo revista': titulo_revista,
                                'ISSN': issn,
                                'Editorial': editorial,
                                'Volumen': volumen,
                                'Fascículo': fasciculo,
                                'Páginas': paginas,
                                'Año publicación': año,
                                'DOI': doi,
                                'Palabras clave': palabras,
                                'Areas': areas,
                                'Sectores': sectores
                            }
                            integrante_data['Publicaciones'].append(publicacion_data)
                            writer.writerow([grupo, enlace_grupo, lider, cvlac_lider, nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria, titulo_publicacion, nombres_integrantes, tipo_producto, tipo_publicacion, estado, pais, titulo_revista, issn, editorial, volumen, fasciculo, paginas, año, doi, palabras, areas, sectores])
                        grupo_data['Integrantes'].append(integrante_data)
                data_json.append(grupo_data)

        # Insertar datos en MongoDB Atlas
        '''
        try:
            result = collection.insert_many(data_json)
            print(f"Se insertaron {len(result.inserted_ids)} documentos en MongoDB Atlas")
        except Exception as e:
            print(f"Error al insertar documentos en MongoDB Atlas: {e}")

        print("Resultados almacenados en", archivo_salida_csv, "y en MongoDB Atlas")
        '''

except requests.exceptions.ConnectionError as e:
    print("Error de conexión:", e)

'''
finally:
    client.close()
'''