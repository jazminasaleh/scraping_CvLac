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
url = 'https://scienti.minciencias.gov.co/ciencia-war/busquedaGrupoXInstitucionGrupos.do?codInst=930&sglPais=&sgDepartamento=&maxRows=152&grupos_tr_=true&grupos_p_=1&grupos_mr_=1'

# Los resultados se van a almacenar en un csv con nombre resultados_grupos
archivo_salida_json = 'resultados_grupos_json.json'
archivo_salida_csv = 'resultados_grupos_csv.csv'

#Conexion con mongodb
"""""
MONGO_URI = "mongodb+srv://juanitasanabria:XwFAqnuDWYryhzab@cvlacdb.tbchf.mongodb.net/"


try:
    client = MongoClient(MONGO_URI)
    db = client.cvlacdb  # Nombre de la base de datos
    collection = db.grupos  # Nombre de la colección
    print("Conexión a MongoDB Atlas establecida con éxito")
except ConnectionFailure:
    print("No se pudo conectar a MongoDB Atlas")
"""""

def cargar_paises_espanol():
    paises = []
    with open('paises_espanol.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            paises.append(row[0])
    return paises

paises_espanol = cargar_paises_espanol()
def cargar_meses():
    meses = {}
    with open('meses.csv', mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            meses[row['mes']] = row['numero']  
    return meses
meses=cargar_meses()

def formatear_fecha(fecha_texto,meses):
    partes = fecha_texto.split("de")
    if len(partes) == 2:  
        mes = partes[0].strip()  
        anio = partes[1].strip()  
        mes_num = meses.get(mes, "01") 
        return f"01/{mes_num}/{anio}"
    elif len(partes) == 1:  
        anio = partes[0].strip()
        return f"01/01/{anio}"  
    return None
# Función para extrear la informcaión de los grupos y sus investigadores
def procesar_grupo(fila):
    ano_mes_formacion_grupo  = ""
    departamento_grupo = ""
    ciudad_grupo = ""
    paginaweb_grupo = ""
    email_grupo = ""
    clasificacion_grupo = ""
    areas_grupo = ""
    programacion_grupo = ""
    programacion_secundaria_grupo = "" 
    instituciones_avaladas_str = ""
    instituciones_no_avaladas_str=""
    lineas_investigacion_str = ""
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
                ano_mes_formacion_grupo  = ""
                departamento_grupo = ""
                ciudad_grupo = ""
                paginaweb_grupo = ""
                email_grupo = ""
                clasificacion_grupo = ""
                areas_grupo = ""
                programacion_grupo = ""
                programacion_secundaria_grupo = "" 
                instituciones_str = ""
                lineas_investigacion_str = ""
                for table in tables:
                    primer_tr = table.find('tr')
                    primer_td = primer_tr.find('td')
                    if primer_td and primer_td.text.strip() == "Datos básicos":
                        filas = table.find_all('tr')
                        for fila in filas:
                            celdas = fila.find_all('td')
                            if len(celdas) == 2:
                                etiqueta = celdas[0].text.strip()
                                valor = celdas[1].text.strip()
                                    
                                if etiqueta == "Año y mes de formación":
                                    if valor:
                                        ano_mes_formacion_grupo = valor
                                    else:
                                        ano_mes_formacion_grupo = ""
                                elif etiqueta == "Departamento - Ciudad":
                                    if valor:
                                        ciudad_grupo = valor
                                        if "-" in ciudad_grupo:
                                            # Separa la cadena en dos partes, antes y después del "-"
                                            partes = ciudad_grupo.split("-")
                                            # Guarda la primera parte en departamento y la segunda en ciudad_grupo
                                            departamento_grupo = partes[0].strip()
                                            ciudad_grupo = partes[1].strip()
                                        else:
                                            # Si no hay "-", guarda todo el valor en ciudad_grupo
                                            departamento_grupo = ""
                                            ciudad_grupo = valor.strip()
                                    else:
                                        departamento_grupo = ""
                                        ciudad_grupo = ""
                                elif etiqueta == "Página web":
                                    if valor:
                                        paginaweb_grupo = valor
                                    else:
                                        paginaweb_grupo = ""
                                elif etiqueta == "E-mail":
                                    if valor: 
                                        email_grupo = valor
                                    else:
                                        email_grupo = ""
                                elif etiqueta == "Clasificación":
                                    if valor:
                                        clasificacion_grupo = valor[0]
                                    else:
                                        clasificacion_grupo = "Sin clasificar"
                                elif etiqueta == "Área de conocimiento":
                                    if valor:
                                        areas_grupo = valor
                                    else:
                                        areas_grupo = ""
                                elif etiqueta == "Programa nacional de ciencia y tecnología":
                                    if valor:
                                        programacion_grupo = valor
                                    else:
                                        programacion_grupo = ""
                                elif etiqueta == "Programa nacional de ciencia y tecnología (secundario)":
                                    if valor:
                                        programacion_secundaria_grupo = valor
                                    else:
                                        programacion_secundaria_grupo = ""
                    if primer_td and primer_td.text.strip() == "Instituciones":
                        filas = table.find_all('tr')
                        instituciones_avaladas = []
                        instituciones_no_avaladas = []
                        for fila in filas[1:]:  # Empezamos desde la segunda fila para omitir el encabezado
                            celdas = fila.find_all('td')
                            if len(celdas) > 0:
                                institucion = celdas[0].text.strip()
                                institucion_limpia = re.sub(r'^\d+\.-?\s*', '', institucion)
           
                                # Usar una expresión regular para detectar si está "Avalado" o "No Avalado"
                                if re.search(r'\(Avalado\)', institucion, re.IGNORECASE):
                                    institucion_limpia = re.sub(r'\s*-\s*\(Avalado\)', '', institucion_limpia).strip()
                                    instituciones_avaladas.append(institucion_limpia)
                                elif re.search(r'\(No Avalado\)', institucion, re.IGNORECASE):
                                    institucion_limpia = re.sub(r'\s*-\s*\(No Avalado\)', '', institucion_limpia).strip()
                                    instituciones_no_avaladas.append(institucion_limpia)
                                    
                                           
                        # Unir todas las líneas en una sola cadena, separadas por comas
                        instituciones_avaladas_str = ", ".join(instituciones_avaladas)
                        instituciones_no_avaladas_str = ", ".join(instituciones_no_avaladas)

                    if primer_td and primer_td.text.strip() == "Líneas de investigación declaradas por el grupo":
                        filas = table.find_all('tr')
                        lineas_investigacion = []
                        for fila in filas[1:]:  # Empezamos desde la segunda fila para omitir el encabezado
                            celdas = fila.find_all('td')
                            if len(celdas) > 0:
                                linea = celdas[0].text.strip()
                                linea_limpia = re.sub(r'^\d+\.-?\s*', '', linea)
                                if linea_limpia:
                                    lineas_investigacion.append(linea_limpia)
                        
                        # Unir todas las líneas en una sola cadena, separadas por comas
                        lineas_investigacion_str = ", ".join(lineas_investigacion)

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
                                    area_actuacion=''
                                    formacion_academica=[]
                                    patentes=[]
                                    publicaciones= []
                                    lineas_activas = []
                                    lineas_no_activas = []
                                    

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
                                        if sexo == '':
                                            sexo = 'No tiene'

                                        categoria_td = table_cvlac.find('td', string='Categoría')
                                        if categoria_td:
                                            categoria = categoria_td.find_next('td').text.strip()
                                            categoria = ' '.join(categoria.split()) #quitar los espacios adicionales
                                            ultimo_paren = categoria.rfind(')')
                                            if ultimo_paren != -1:
                                                categoria = categoria[:ultimo_paren + 1].strip()
                                        if categoria == '':
                                            categoria = 'Sin categoría'
                                        #Sección Formación académica
                                        seccion_formacion=table_cvlac.find('h3',string=['Formación Académica'])
                                        if seccion_formacion:
                                            fila_formacion=seccion_formacion.find_parent('tr').find_next_sibling('tr')

                                            while fila_formacion:
                                                celdas_formacion=fila_formacion.find_all('td')

                                                if len(celdas_formacion)>1:
                                                    tipo_formacion=celdas_formacion[1].find('b').text.strip()
                                                    contenido= celdas_formacion[1].get_text(separator="|").strip().split("|")
                                                    institucion=contenido[1].strip() if len(contenido) > 1 else None
                                                    titulo_formacion = contenido[2].strip() if len(contenido) > 2 else None
                                                    inicio_formacion = contenido[3].strip() if len(contenido) > 3 else None
                                                    trabajo_grado = contenido[4].strip() if len(contenido) > 4 else None

                                                    if inicio_formacion:
                                                        fechas_formacion=inicio_formacion.split("-")
                                                        inicio_formacion=formatear_fecha(fechas_formacion[0].strip(),meses) if len(fechas_formacion) > 0 else None
                                                        fin_formacion= formatear_fecha(fechas_formacion[1].strip(),meses) if len(fechas_formacion) > 1 else None 
                                                    if (tipo_formacion, institucion,titulo_formacion,inicio_formacion,fin_formacion,trabajo_grado) not in formacion_academica:
                                                        formacion_academica.append((tipo_formacion, institucion,titulo_formacion,inicio_formacion,fin_formacion,trabajo_grado))

                                                fila_formacion=fila_formacion.find_next_sibling('tr')
                             
                                        #Sección de patentes
                                        seccion_patentes = table_cvlac.find('h3', string=['Patentes'])
                                        if seccion_patentes:
                                            fila_patentes = seccion_patentes.find_parent('tr').find_next_sibling('tr')

                                            while fila_patentes:
                                                celdas_patentes = fila_patentes.find_all('td')
                                               

                                                if len(celdas_patentes) > 0:
                                                    img_tag = celdas_patentes[0].find('img')
                                                    estado_patente = 'Vigente' if img_tag else 'No Vigente'
                                                    
                                                    tipo_patente_elemento = celdas_patentes[0].find('b')
                                                    if tipo_patente_elemento:
                                                        tipo_patente = tipo_patente_elemento.text.strip()
                                                    else:
                                                        tipo_patente = None
                                                    siguiente_fila = fila_patentes.find_next_sibling('tr')
                                                    if siguiente_fila:
                                                        blockquote = siguiente_fila.find('blockquote')
                                                        if blockquote:
                                                           
                                                            contenido = blockquote.get_text(separator="|").strip().split('|')
                                                            
                                                            codigo_patente = contenido[0].split(' - ')[0].strip()
                                                            titulo_patente = contenido[0].split(' - ')[1].strip()
                                                            
                                                            institucion = None
                                                            via_solicitud_patente = None
                                                            pais_patente = None
                                                            fecha_patente = None
                                                            nombre_solicitante_patente = None
                                                            gaceta_publicacion_patente = None
                                                            
                                                            etiquetas_i = blockquote.find_all('i')
                                                            for i in range(len(etiquetas_i)):
                                                                if 'Institución' in etiquetas_i[i].text:
                                                                    
                                                                    siguiente_texto = etiquetas_i[i].next_sibling
                                                                   
                                                                    if siguiente_texto:
                                                                        institucion_patente = siguiente_texto.strip().split(',')[0]      

                                                                if 'Vía de solicitud' in etiquetas_i[i].text:
                                                                   
                                                                    siguiente_texto = etiquetas_i[i].next_sibling
                                                                   
                                                                    if siguiente_texto:
                                                                        via_solicitud_patente = siguiente_texto.strip().split('En:')[0]  
                                                                        pais_texto = siguiente_texto.split('En:')[-1].strip()
                                                                        pais_patente = pais_texto.split(',')[0].strip()
                                                                    
                                                                if 'Nombre del solicitante de la patente' in etiquetas_i[i].text:
                                                                    
                                                                    siguiente_texto = etiquetas_i[i].text.split(':')[-1].strip()
                                                                   
                                                                    if siguiente_texto:
                                                                        nombre_solicitante_patente = siguiente_texto.strip().split(',')[0]  
                                                                
                                                                if 'Gaceta Industrial de Publicación' in etiquetas_i[i].text:
                                                                    
                                                                    siguiente_texto = etiquetas_i[i].text.split(':')[-1].strip()
                                                                   
                                                                    if siguiente_texto:
                                                                        gaceta_publicacion_patente = siguiente_texto.strip().split(',')[0]  
                                                                
                                                                # Identificar la fecha de la patente (YYYY-MM-DD)
                                                                if re.search(r'\d{4}-\d{2}-\d{2}', siguiente_texto):
                                                                    fecha_patente = re.search(r'\d{4}-\d{2}-\d{2}', siguiente_texto).group(0)


                                                        if(tipo_patente,estado_patente,codigo_patente,titulo_patente,institucion_patente, via_solicitud_patente, pais_patente, fecha_patente, nombre_solicitante_patente, gaceta_publicacion_patente) not in patentes:
                                                            patentes.append((tipo_patente,estado_patente,codigo_patente,titulo_patente,institucion_patente,  via_solicitud_patente, pais_patente, fecha_patente, nombre_solicitante_patente, gaceta_publicacion_patente))        
                                                           
                                                fila_patentes = fila_patentes.find_next_sibling('tr')


                                        #Sección Áreas de Actuación
                                        seccion_area_actuacion=table_cvlac.find('h3',string=['Áreas de actuación'])
                                        if seccion_area_actuacion:
                                            
                                            fila_area=seccion_area_actuacion.find_parent('tr').find_next_sibling('tr')
                                            texto_unico = set()  
                                            while fila_area:
                                                celdas_area = fila_area.find_all('td')
                                                for celda in celdas_area:
                                                    texto_unico.update(celda.get_text(strip=True).split(' -- ')) 
                                                fila_area = fila_area.find_next_sibling('tr')
                                            area_actuacion = ','.join(sorted(texto_unico))  
                                        # Sección Líneas de Investigación
                                        seccion_lineas_investigacion = table_cvlac.find('h3', string=['Líneas de investigación'])
                                        if seccion_lineas_investigacion:
                                            fila_linea = seccion_lineas_investigacion.find_parent('tr').find_next_sibling('tr')

                                            lineas_activas = []
                                            lineas_no_activas = []

                                            while fila_linea:
                                                celdas_linea = fila_linea.find_all('td')
                                                for celda in celdas_linea:
                                                    texto_linea = celda.get_text(strip=True).rstrip(',')
                                                    if "Activa:Si" in texto_linea:
                                                        lineas_activas.append(texto_linea.replace("Activa:Si", "").strip().rstrip(','))
                                                    elif "Activa:No" in texto_linea:
                                                        lineas_no_activas.append(texto_linea.replace("Activa:No", "").strip().rstrip(','))
                                                fila_linea = fila_linea.find_next_sibling('tr')
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
                                                                texto= texto_publicacion.get_text()
                                                                if texto in ["Palabras: ","Areas: ","Sectores: "]:
                                                                     tipo_publicacion="Capítulo de Libro"
                                                                else: 
                                                                 tipo_publicacion=texto.split(' - ')[-1]
                                                            else:
                                                                tipo_publicacion="Capítulo de Libro"
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
                                                                if titulo_publicacion == '':
                                                                    publicacion_comillas_dobles1 = texto_blockquote.find('""')
                                                                    
                                                                    if publicacion_comillas_dobles1 != -1:
                                                                       publicacion_comillas_dobles2 =  texto_blockquote.find('" .', publicacion_comillas_dobles1 + 3)
                                                                       publicacion_comillas_dobles2_secundario =  texto_blockquote.find('." En:', publicacion_comillas_dobles1 + 3)
                                                                       publicacion_comillas_dobles2_tercero =  texto_blockquote.find('""', publicacion_comillas_dobles1 + 3)
                                                                       publicacion_comillas_dobles2_cuarto =  texto_blockquote.find('"', publicacion_comillas_dobles1 + 3)
                                                                       if publicacion_comillas_dobles2 != -1:
                                                                        titulo_publicacion = texto_blockquote[publicacion_comillas_dobles1 + 1:publicacion_comillas_dobles2]
                                                                        titulo_publicacion = titulo_publicacion.strip('"')
                                                                       elif publicacion_comillas_dobles2_secundario != -1:
                                                                            titulo_publicacion = texto_blockquote[publicacion_comillas_dobles1 + 1:publicacion_comillas_dobles2_secundario]
                                                                            titulo_publicacion = titulo_publicacion.strip('"')
                                                                       elif publicacion_comillas_dobles2_tercero != -1:
                                                                            titulo_publicacion = texto_blockquote[publicacion_comillas_dobles1 + 1:publicacion_comillas_dobles2_tercero]
                                                                            titulo_publicacion = titulo_publicacion.strip('"')
                                                                       elif publicacion_comillas_dobles2_cuarto != -1:
                                                                            titulo_publicacion = texto_blockquote[publicacion_comillas_dobles1 + 1:publicacion_comillas_dobles2_cuarto]
                                                                            titulo_publicacion = titulo_publicacion.strip('"')
                                                                    publicacion_comillas_dobles_separadas1 = texto_blockquote.find('" "')
                                                                    if publicacion_comillas_dobles_separadas1 != -1:
                                                                       publicacion_comillas_dobles_separadas2 =  texto_blockquote.find('" .', publicacion_comillas_dobles_separadas1 + 2)
                                                                       publicacion_comillas_dobles_separadas2_segundas =  texto_blockquote.find('""', publicacion_comillas_dobles_separadas1 + 2)
                                                                       publicacion_comillas_dobles_separadas2_terceras =  texto_blockquote.find('. En:', publicacion_comillas_dobles_separadas1 + 3)
                                                                       publicacion_comillas_dobles_separadas2_cuartas =  texto_blockquote.find('"" . En:', publicacion_comillas_dobles_separadas1 + 3)
                                                                       if publicacion_comillas_dobles2 != -1:
                                                                        titulo_publicacion = texto_blockquote[publicacion_comillas_dobles_separadas1 + 1:publicacion_comillas_dobles_separadas2]
                                                                        titulo_publicacion = titulo_publicacion.strip('"')
                                                                       elif publicacion_comillas_dobles_separadas2_segundas != -1:
                                                                        titulo_publicacion = texto_blockquote[publicacion_comillas_dobles_separadas1 + 1:publicacion_comillas_dobles_separadas2_segundas]
                                                                        titulo_publicacion = titulo_publicacion.strip('"')
                                                                       elif publicacion_comillas_dobles_separadas2_terceras != -1:
                                                                        titulo_publicacion = texto_blockquote[publicacion_comillas_dobles_separadas1 + 1:publicacion_comillas_dobles_separadas2_terceras]
                                                                        titulo_publicacion = titulo_publicacion.strip('"')
                                                                       elif publicacion_comillas_dobles_separadas2_cuartas != -1:
                                                                        titulo_publicacion = texto_blockquote[publicacion_comillas_dobles_separadas1 + 1:publicacion_comillas_dobles_separadas2_cuartas]
                                                                        titulo_publicacion = titulo_publicacion.strip('"')
                                                                    
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
                                                                                if "revista" in titulo_revista.lower():
                                                                                    indice_revista = titulo_revista.lower().find("revista")
                                                                                    titulo_revista = titulo_revista[indice_revista:].strip()
                                                                                if titulo_revista.startswith("No Aplica"):
                                                                                    titulo_revista = titulo_revista[9:].strip()
                                                                                elif titulo_revista.startswith('" . En:'):
                                                                                    titulo_revista = titulo_revista[7:].strip()
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
                                                                    elif tipo_producto == "Capitulos de libro":
                                                                        if len(pais) == 0:
                                                                            indice_final_pais = texto_blockquote.find("En:") + 3
                                                                            indice_issn = texto_blockquote.find("ISSN:")
                                                                            indice_isbn = texto_blockquote.find("ISBN:")
                                                                            
                                                                            if indice_issn != -1 and (indice_isbn == -1 or indice_issn < indice_isbn):
                                                                                indice_final_dato = indice_issn
                                                                            elif indice_isbn != -1:
                                                                                indice_final_dato = indice_isbn
                                                                            else:
                                                                                indice_final_dato = -1
                                                                            
                                                                            if indice_final_pais != -1 and indice_final_dato != -1:
                                                                                # Extraer texto entre 'En:' y el próximo 'ISSN' o 'ISBN'
                                                                                texto_despues_pais = texto_blockquote[indice_final_pais:indice_final_dato].strip()
                                                                                
                                                                                paises_validos = [p.lower() for p in paises_espanol]
                                                                                pais_encontrado = False
                                                                                
                                                                                for pais_valido in paises_validos:
                                                                                    nombre_pais = texto_despues_pais.lower().strip()  # Asegurarse de eliminar espacios adicionales
                                                                                    
                                                                                    if (nombre_pais == pais_valido or 
                                                                                        nombre_pais.startswith(pais_valido + " ") or 
                                                                                        nombre_pais.startswith(pais_valido)):
                                                                                        pais = pais_valido.capitalize()
                                                                                        pais_encontrado = True
                                                                                        break
                                                                                
                                                                                if not pais_encontrado:
                                                                                    pais = ""
                                                                            else:
                                                                                pais = ""
                                                                    else:
                                                                        titulo_revista = ""
                                                                issn = ''
                                                                isbn = ''
                                                                nombre_libro=''
                                                                issn = obtener_issn(texto_blockquote)  
                                                                isbn = obtener_isbn(texto_blockquote)
                                                                editorial = obtener_editorial(texto_blockquote)
                                                                volumen = obtener_volumen(texto_blockquote)
                                                                fasciculo = obtener_fasciculo(texto_blockquote)
                                                                paginas = obtener_paginas(texto_blockquote)
                                                                if  tipo_producto == "Artículos" :
                                                                    año=obtener_año(texto_blockquote)
                                                                elif  tipo_producto == "Libros":
                                                                    año=obtener_año_libros(texto_blockquote)
                                                                elif tipo_producto =="Capitulos de libro":
                                                                    año=obtener_año_capitulos(texto_blockquote)
                                                                    nombre_libro=obtener_nombre_libro(texto_blockquote)
                                                                elif tipo_producto=="Textos en publicaciones no científicas":
                                                                    año = obtener_año_en_textos(texto_blockquote)
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
                                                                   publicaciones.append((titulo_publicacion, nombres_integrantes_str,  tipo_producto, tipo_publicacion, estado, pais, titulo_revista,nombre_libro, issn,isbn, editorial, volumen, fasciculo, paginas, año, doi, palabras, areas, sectores))
                                                                   
                                                #Va a la siguinete publicacion               
                                                fila_publicacion = fila_publicacion.find_next_sibling('tr')
                                               
                                    # Agregar los datos del integrante y sus artículos a la lista
                                    integrantes.append([nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria,formacion_academica,area_actuacion,lineas_activas,lineas_no_activas,publicaciones,patentes])

                                except requests.exceptions.RequestException:
                                    # En caso de error en la solicitud HTTP
                                    integrantes.append(['', '', '', '', '', '',[],'',[],[],[],[]])

            except requests.exceptions.RequestException:
                # En caso de error en la solicitud HTTP
                integrantes = []

            # Devolver los datos del grupo y sus integrantes
            return [nombre_grupo, enlace_gruplac_grupo, ano_mes_formacion_grupo, departamento_grupo, ciudad_grupo, paginaweb_grupo, email_grupo, clasificacion_grupo, areas_grupo, programacion_grupo, programacion_secundaria_grupo, instituciones_avaladas_str,  instituciones_no_avaladas_str, lineas_investigacion_str, nombre_lider, cvlac_lider, integrantes]

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

def obtener_isbn(texto_blockquote):
    indice_isbn = texto_blockquote.find("ISBN:")
    if indice_isbn != -1:
        indice_v = texto_blockquote.find("v.", indice_isbn)
        indice_ed = texto_blockquote.find("ed:", indice_isbn)
        indice_p = texto_blockquote.find("p.", indice_isbn)
        
        topes = [i for i in [indice_v, indice_ed, indice_p] if i != -1]
        if topes:
            primer_tope = min(topes)
            return texto_blockquote[indice_isbn + len("ISBN:"):primer_tope].strip().strip(',')
        else:
            return texto_blockquote[indice_isbn + len("ISBN:"):].strip().strip(',')
    
    return ""

def obtener_nombre_libro(texto_blockquote):
    indice_inicio_comillas = texto_blockquote.find('"')
    indice_fin_comillas = texto_blockquote.find('"', indice_inicio_comillas + 1)
    indice_en = texto_blockquote.find("En:", indice_fin_comillas)
    
    if indice_inicio_comillas != -1 and indice_fin_comillas != -1 and indice_en != -1:
        texto_despues_comillas = texto_blockquote[indice_fin_comillas + 1:indice_en].strip()
        
        if texto_despues_comillas and len(texto_despues_comillas) > 1 and texto_despues_comillas != ".":
            return texto_despues_comillas
    
    return ""

def obtener_editorial(texto_blockquote):
    indice_ed = texto_blockquote.find("ed:")
    
    if indice_ed != -1:
        indice_v = texto_blockquote.find("v.", indice_ed)
        indice_isbn_despues = texto_blockquote.find("ISBN:", indice_ed)
        
        if indice_v != -1 and (indice_isbn_despues == -1 or indice_v < indice_isbn_despues):
            editorial = texto_blockquote[indice_ed + len("ed:"):indice_v].strip()
        elif indice_isbn_despues != -1:
            editorial = texto_blockquote[indice_ed + len("ed:"):indice_isbn_despues].strip()
        else:
            editorial = texto_blockquote[indice_ed + len("ed:"):].strip()
        
        if editorial.endswith(','):
            editorial = editorial[:-1].strip()
        
        return editorial
    
    return ""

def obtener_volumen(texto_blockquote):
    indice_v = texto_blockquote.find("v.")
    
    if indice_v == -1:
        indice_isbn = texto_blockquote.find("ISBN")
        if indice_isbn != -1:
            indice_v = texto_blockquote.find("v. ", indice_isbn)
    
    if indice_v != -1:
        indice_fasc = texto_blockquote.find("fasc.", indice_v)
        indice_palabras_clave = texto_blockquote.find("Palabras:", indice_v)
        topes = [i for i in [indice_fasc, indice_palabras_clave] if i != -1]
        
        if topes:
            primer_tope = min(topes)
            volumen = texto_blockquote[indice_v + len("v."):primer_tope].strip().strip(',')
        else:
            volumen = texto_blockquote[indice_v + len("v."):].strip().strip(',')
        
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
        return "" if fasciculo in ["N/A", "NA", "N7A", "-", "--", "(N/A)", "(N/A"] or (fasciculo.isdigit() and len(fasciculo) == 4) else fasciculo
    return ""

# Obtener el número de pagina
def obtener_paginas(texto_blockquote):
    patron_pagina = r'(?:pages?|p\.)\s*(\d+)\s*-\s*(\d+)'
    resultado_pagina = re.search(patron_pagina, texto_blockquote)
    if resultado_pagina:
        return f"{resultado_pagina.group(1)}-{resultado_pagina.group(2)}"
    return ""

def obtener_año(texto_blockquote):
    # Buscamos el patrón de páginas seguido de una coma y luego un posible año
    patron_paginas = re.compile(r'p\.\d+\s*-\s*\d+\s*,\s*(\d{4})')
    match_paginas = patron_paginas.search(texto_blockquote)
    
    if match_paginas:
        posible_año = match_paginas.group(1)
        return posible_año
    
    # Si no encontramos el año después del rango de páginas, continuamos con las búsquedas previas
    indices_comas = [m.start() for m in re.finditer(r',', texto_blockquote)]
    indices_puntos = [m.start() for m in re.finditer(r'\. ', texto_blockquote)]
    
    # Búsqueda de un posible año entre comas
    for i in range(len(indices_comas) - 1):
        posible_año = texto_blockquote[indices_comas[i] + 1:indices_comas[i + 1]].strip()
        if posible_año.isdigit() and len(posible_año) == 4:
            return posible_año
    
    # Búsqueda de un posible año después de un punto
    for indice_punto in indices_puntos:
        posible_año = texto_blockquote[indice_punto + 2:indice_punto + 6]
        if posible_año.isdigit():
            return posible_año
    return ""

def obtener_año_libros(texto_blockquote):
    patron = r"En:\s*.*?(\d{4})\."
    coincidencia = re.search(patron, texto_blockquote)
    
    if coincidencia:
        return coincidencia.group(1)
    return ""

def obtener_año_capitulos(texto_blockquote):
    patron = r",\s*(\d{4})(?!\d)"
    coincidencias = re.findall(patron, texto_blockquote.strip())
    
    if coincidencias:
        return coincidencias[-1]
    return ""

def obtener_año_en_textos(texto_blockquote):
    patron = r"En:\s*[^.]+\.\s*(\d{4})"
    coincidencia = re.search(patron, texto_blockquote)
    
    if coincidencia:
        return coincidencia.group(1)
    return ""
    
def obtener_doi(texto_blockquote):
    indice_doi = texto_blockquote.find("DOI:")
    if indice_doi != -1:
        indice_palabras = texto_blockquote.find("Palabras:", indice_doi)
        indice_sectores = texto_blockquote.find("Sectores:", indice_doi)
        indice_doi_dos = texto_blockquote.find("doi:", indice_doi)
        if indice_palabras != -1:
            doi = texto_blockquote[indice_doi + len("DOI:"):indice_palabras].strip()
        elif indice_sectores != -1:
            doi = texto_blockquote[indice_doi + len("DOI:"):indice_sectores].strip()
        elif indice_doi_dos != -1:
            doi = texto_blockquote[indice_doi_dos + len("doi:"):].strip()
        else:
            doi = texto_blockquote[indice_doi + len("DOI:"):].strip()
        return "" if doi == "N/A" else doi
    return ""

# Obtener palabras claves de la publicación
def obtener_palabras_clave(texto_blockquote):
    indice_palabras = texto_blockquote.find("Palabras:")
    if indice_palabras != -1:
        indice_sectores = texto_blockquote.find("Sectores:", indice_palabras)
        indice_areas = texto_blockquote.find("Areas:", indice_palabras)
        
        if indice_sectores != -1 and (indice_sectores < indice_areas or indice_areas == -1):
            fin_palabras = indice_sectores
        elif indice_areas != -1:
            fin_palabras = indice_areas
        else:
            fin_palabras = len(texto_blockquote)
        
        palabras_texto = texto_blockquote[indice_palabras + len("Palabras:"):fin_palabras].strip()
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
    partes = texto_blockquote[:indice_comilla1].split('Tipo: ')
    nombres_integrantes_limpios = []
    
    for parte in partes:
        if not parte.strip():
            continue
        
        indice_inicio_nombre = parte.find('publicado')
        if indice_inicio_nombre != -1:
            nombre = parte[indice_inicio_nombre + len('publicado'):].strip()
        else:
            indice_inicio_nombre = parte.find('Capítulo de libro')
            if indice_inicio_nombre != -1:
                nombre = parte[indice_inicio_nombre + len('Capítulo de libro'):].strip()
            else:
                nombre = parte.strip()

        nombre_limpio = re.sub(r"['\\]", '', nombre).strip(',')
        
        if "Capítulo de libro" in nombre_limpio:
            continue
        
        if nombre_limpio and not nombre_limpio.isspace():
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
        writer.writerow(['Nombre del grupo', 'Enlace al GrupLac', 'Fecha de formcación', 'Departamento', 'Ciudad',  'Página web', 'E-mail', 'Clasificación', 'Área de conocimiento','Programa nacional', 'Programa nacional(secundario)', 'Instituciones avaladas','Instituciones no avaladas',  'Líneas de investigación',  'Nombre del líder', 'Enlace al CvLac líder',
                         'Nombre del integrante', 'Enlace al CvLac del investigador', 'Nombre en citaciones',
                         'Nacionalidad', 'Sexo', 'Categoría','Tipo de Formación','Institución','Título Formación','Inicio Formación','Fin Formación','Trabajo de Grado','Áreas de Actuación','Líneas Activas','Líneas no Activas','Título publicación', 'Integrantes involucrados',
                         'Tipo producto', 'Tipo publicación', 'Estado', 'País', 'Titulo revista', 'Nombre Libro','ISSN','ISBN',
                         'Editorial', 'Volumen', 'Fascículo', 'Páginas', 'Año publicación', 'DOI', 'Palabras clave',
                         'Areas', 'Sectores','Tipo de Patente','Estado Patente','Código de Patente','Título Patente','Institución de Patente', 'Vía de solicitud de patente', 'Pais patente', 'Fecha patente', 'Nombre del solicitante de la patente', 'Gaceta Industrial de Publicación de patente'])

        # Crear hilos para procesar los grupos
        with ThreadPoolExecutor() as executor:
            # Mapear la función procesar_grupo a cada fila de la tabla
            resultados = list(executor.map(procesar_grupo, filas))

        for datos in resultados:
            if datos:
                grupo, enlace_grupo,  ano, departamento,  ciudad,  pagina_web, email, clasificacion, areas_grupo, programa, programa_secundario,  instituciones_avaladas_str, instituciones_no_avaladas_str, lineas_investigacion_str, lider, cvlac_lider, integrantes = datos
                grupo_data = {
                    'Nombre del grupo': grupo,
                    'Enlace al GrupLac': enlace_grupo,
                    'Fecha de formcación': ano, 
                    'Departamento': departamento,
                    'Ciudad': ciudad,
                    'Página web': pagina_web,
                    'E-mail': email,
                    'Clasificación': clasificacion,
                    'Área de conocimiento': areas_grupo,
                    'Programa nacional': programa,
                    'Programa nacional(secundario)': programa_secundario,
                    'Instituciones avaladas': instituciones_avaladas_str,
                    'Instituciones no avaladas': instituciones_no_avaladas_str,
                    'Líneas de investigación': lineas_investigacion_str,
                    'Nombre del líder': lider,
                    'Enlace al CvLac líder': cvlac_lider,
                    'Integrantes': []
                }
                for integrante in integrantes:
                    if len(integrante) == 12:
                        nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria,formacion_academica,area_actuacion,lineas_activas,lineas_no_activas,publicaciones,patentes = integrante
                        integrante_data = {
                            'Nombre del integrante': nombre_integrante,
                            'Enlace al CvLac del investigador': enlace_cvlac_integrante,
                            'Nombre en citaciones': nombre_citaciones,
                            'Nacionalidad': nacionalidad,
                            'Sexo': sexo,
                            'Categoría': categoria,
                            'Formación Académica':[],
                            'Áreas de Actuación':area_actuacion,
                            'Líneas Activas':lineas_activas,
                            'Líneas no Activas':lineas_no_activas,
                            'Publicaciones': [],
                            'Patentes':[]
                        }
                        for formacion in formacion_academica:
                            tipo_formacion,institucion,titulo_formacion,inicio_formacion,fin_formacion,trabajo_grado = formacion
                            formacion_data = {
                                'Tipo Formación': tipo_formacion,
                                'Institucion': institucion,
                                'Título Formacion':titulo_formacion,
                                'Inicio Formación':inicio_formacion,
                                'Fin Formación':fin_formacion,
                                'Trabajo de Grado':trabajo_grado
                            }
                            writer.writerow([grupo, enlace_grupo, ano, departamento, ciudad, pagina_web, email, clasificacion, areas_grupo, programa, programa_secundario, instituciones_avaladas_str, instituciones_no_avaladas_str, lineas_investigacion_str, lider, cvlac_lider, nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria,tipo_formacion,institucion,titulo_formacion,inicio_formacion,fin_formacion,trabajo_grado,area_actuacion,lineas_activas,lineas_no_activas,'','', '', '', '', '', '','','', '','', '', '', '', '', '', '', '', '','','','','',''])
                        for publicacion in publicaciones:
                            titulo_publicacion, nombres_integrantes, tipo_producto, tipo_publicacion, estado, pais, titulo_revista, nombre_libro,issn,isbn, editorial, volumen, fasciculo, paginas, año, doi, palabras, areas, sectores = publicacion
                            publicacion_data = {
                                'Título publicación': titulo_publicacion,
                                'Integrantes involucrados': nombres_integrantes,
                                'Tipo producto': tipo_producto,
                                'Tipo publicación': tipo_publicacion,
                                'Estado': estado,
                                'País': pais,
                                'Titulo revista': titulo_revista,
                                'Nombre Libro':nombre_libro,
                                'ISSN': issn,
                                'ISBN':isbn,
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
                            writer.writerow([grupo, enlace_grupo, ano, departamento, ciudad, pagina_web, email, clasificacion, areas_grupo, programa, programa_secundario, instituciones_avaladas_str, instituciones_no_avaladas_str, lineas_investigacion_str, lider, cvlac_lider, nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria, '', '', '', '', '', '', area_actuacion, ', '.join(lineas_activas), ', '.join(lineas_no_activas), titulo_publicacion, nombres_integrantes, tipo_producto, tipo_publicacion, estado, pais, titulo_revista, nombre_libro, issn, isbn, editorial, volumen, fasciculo, paginas, año, doi, palabras, areas, sectores] + [''] * 3)
                        for patente in patentes:
                            tipo_patente,estado_patente,codigo_patente,titulo_patente,institucion_patente,  via_solicitud_patente, pais_patente, fecha_patente, nombre_solicitante_patente, gaceta_publicacion_patente=patente
                            patente_data={
                                'Tipo de Patente':tipo_patente,
                                'Estado Patente':estado_patente,
                                'Código de Patente':codigo_patente,
                                'Título Patente': titulo_patente,
                                'Institución de Patente': institucion_patente,
                                'Vía de solicitud de patente': via_solicitud_patente, 
                                'Pais patente':  pais_patente, 
                                'Fecha patente': fecha_patente, 
                                'Nombre del solicitante de la patente': nombre_solicitante_patente, 
                                'Gaceta Industrial de Publicación de patente': gaceta_publicacion_patente
                            }
                            integrante_data['Formación Académica'].append(formacion_data)
                            integrante_data['Publicaciones'].append(publicacion_data)
                            integrante_data['Patentes'].append(patente_data)
                            writer.writerow([grupo, enlace_grupo, ano, departamento, ciudad, pagina_web, email, clasificacion, areas_grupo, programa, programa_secundario, instituciones_avaladas_str, instituciones_no_avaladas_str, lineas_investigacion_str, lider, cvlac_lider, nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria,tipo_formacion,institucion,titulo_formacion,inicio_formacion,fin_formacion,trabajo_grado,area_actuacion,lineas_activas,lineas_no_activas,titulo_publicacion, nombres_integrantes, tipo_producto, tipo_publicacion, estado, pais, titulo_revista,nombre_libro,issn, isbn,editorial, volumen, fasciculo, paginas, año, doi, palabras, areas, sectores,tipo_patente,estado_patente,codigo_patente,titulo_patente,institucion_patente,  via_solicitud_patente, pais_patente, fecha_patente, nombre_solicitante_patente, gaceta_publicacion_patente])
                        grupo_data['Integrantes'].append(integrante_data)
                data_json.append(grupo_data)

        # Insertar datos en MongoDB Atlas
        """""
        try:
            result = collection.insert_many(data_json)
            print(f"Se insertaron {len(result.inserted_ids)} documentos en MongoDB Atlas")
        except Exception as e:
            print(f"Error al insertar documentos en MongoDB Atlas: {e}")

        print("Resultados almacenados en", archivo_salida_csv, "y en MongoDB Atlas")
        
        """""
        """""
        #  actualizacion 

        for grupo_data in data_json:
            filtro_grupo = {'Nombre del grupo': grupo_data['Nombre del grupo']}
            cambios_grupo = {
                '$set': {
                    'Enlace al GrupLac': grupo_data['Enlace al GrupLac'],
                    'Fecha de formcación': grupo_data['Fecha de formcación'],
                    'Departamento - ciudad': grupo_data['Departamento - ciudad'],
                    'Clasificación': grupo_data['Clasificación'],
                    'Área de conocimiento': grupo_data['Área de conocimiento'],
                    'Nombre del líder': grupo_data['Nombre del líder'],
                    'Enlace al CvLac líder': grupo_data['Enlace al CvLac líder'],
                }
            }

            # Actualizar el grupo principal
            resultado_grupo = collection.update_one(filtro_grupo, cambios_grupo, upsert=True)

            if resultado_grupo.matched_count > 0:
                print(f"Grupo actualizado: {grupo_data['Nombre del grupo']}")
            else:
                print(f"Grupo insertado: {grupo_data['Nombre del grupo']}")

            for integrante in grupo_data['Integrantes']:
                filtro_integrante = {'Nombre del grupo': grupo_data['Nombre del grupo'], 'Integrantes.Nombre del integrante': integrante['Nombre del integrante']}
                cambios_integrante = {
                    '$set': {
                        'Integrantes.$.Enlace al CvLac del investigador': integrante['Enlace al CvLac del investigador'],
                        'Integrantes.$.Nombre en citaciones': integrante['Nombre en citaciones'],
                        'Integrantes.$.Nacionalidad': integrante['Nacionalidad'],
                        'Integrantes.$.Sexo': integrante['Sexo'],
                        'Integrantes.$.Categoría': integrante['Categoría'],
                        'Integrantes.$.Publicaciones': integrante['Publicaciones']
                    }
                }

                # Actualizar el investigador específico
                resultado_integrante = collection.update_one(filtro_integrante, cambios_integrante)

                if resultado_integrante.matched_count == 0:
                    # Si no se encontró el integrante, añadirlo al array
                    collection.update_one(
                        {'Nombre del grupo': grupo_data['Nombre del grupo']},
                        {'$push': {'Integrantes': integrante_data}}
                    )
                    print(f"Nuevo integrante añadido: {integrante['Nombre del integrante']}")
                else:
                    print(f"Integrante actualizado: {integrante['Nombre del integrante']}")

                for publicacion in integrante['Publicaciones']:
                    filtro_publicacion = {
                        'Nombre del grupo': grupo_data['Nombre del grupo'],
                        'Integrantes.Nombre del integrante': integrante['Nombre del integrante'],
                        'Integrantes.Publicaciones.Título publicación': publicacion['Título publicación'],
                    }
                    cambios_publicacion = {
                        '$set': {
                            'Integrantes.$[i].Publicaciones.$[j].Integrantes involucrados': publicacion['Integrantes involucrados'],
                            'Integrantes.$[i].Publicaciones.$[j].Tipo producto': publicacion['Tipo producto'],
                            'Integrantes.$[i].Publicaciones.$[j].Tipo publicación': publicacion['Tipo publicación'],
                            'Integrantes.$[i].Publicaciones.$[j].Estado': publicacion['Estado'],
                            'Integrantes.$[i].Publicaciones.$[j].País': publicacion['País'],
                            'Integrantes.$[i].Publicaciones.$[j].Titulo revista': publicacion['Titulo revista'],
                            'Integrantes.$[i].Publicaciones.$[j].ISSN': publicacion['ISSN'],
                            'Integrantes.$[i].Publicaciones.$[j].Editorial': publicacion['Editorial'],
                            'Integrantes.$[i].Publicaciones.$[j].Volumen': publicacion['Volumen'],
                            'Integrantes.$[i].Publicaciones.$[j].Fascículo': publicacion['Fascículo'],
                            'Integrantes.$[i].Publicaciones.$[j].Páginas': publicacion['Páginas'],
                            'Integrantes.$[i].Publicaciones.$[j].Año publicación': publicacion['Año publicación'],
                            'Integrantes.$[i].Publicaciones.$[j].DOI': publicacion['DOI'],
                            'Integrantes.$[i].Publicaciones.$[j].Palabras clave': publicacion['Palabras clave'],
                            'Integrantes.$[i].Publicaciones.$[j].Areas': publicacion['Areas'],
                            'Integrantes.$[i].Publicaciones.$[j].Sectores': publicacion['Sectores']
                        }
                    }
                    opciones = {
                        'arrayFilters': [
                            {'i.Nombre del integrante': integrante['Nombre del integrante']},
                            {'j.Título publicación': publicacion['Título publicación']}
                        ]
                    }

                    # Actualizar la publicación específica
                    resultado_publicacion = collection.update_one(filtro_publicacion, cambios_publicacion, array_filters=opciones['arrayFilters'])

                    if resultado_publicacion.matched_count == 0:
                        # Si no se encontró la publicación, añadirla al array de publicaciones del integrante
                        collection.update_one(
                            {
                                'Nombre del grupo': grupo_data['Nombre del grupo'],
                                'Integrantes.Nombre del integrante': integrante['Nombre del integrante']
                            },
                            {
                                '$push': {
                                    'Integrantes.$.Publicaciones': publicacion_data
                                }
                            }
                        )
                        print(f"Nueva publicación añadida: {publicacion['Título publicación']}")
                    else:
                        print(f"Publicación actualizada: {publicacion['Título publicación']}")
                """""
except requests.exceptions.ConnectionError as e:
    print("Error de conexión:", e)

"""""
finally:
    client.close()
"""""