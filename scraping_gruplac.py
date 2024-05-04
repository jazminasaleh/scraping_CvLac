import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from bs4 import BeautifulSoup, SoupStrainer
import csv
import os
from concurrent.futures import ThreadPoolExecutor

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
archivo_salida = 'resultados_grupos.csv'

# Función para extrear la informcaión de los grupos y sus investigadores
def procesar_grupo(fila):
    columnas = fila.find_all('td')

    # Verificar si hay al mas de tres columnas en la fila
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
                soup_grupo = BeautifulSoup(response_grupo.text, 'lxml', parse_only=SoupStrainer('table'))

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
                                enlace_cvlac_integrante = enlace_integrante.get('href')

                                try:
                                    response_cvlac_integrante = session.get(enlace_cvlac_integrante)
                                    response_cvlac_integrante.raise_for_status()
                                    soup_cvlac_integrante = BeautifulSoup(response_cvlac_integrante.text, 'lxml', parse_only=SoupStrainer('table'))

                                    tables_cvlac = soup_cvlac_integrante.find_all('table')

                                    nombre_citaciones = ''
                                    categoria = ''
                                    nacionalidad = ''
                                    sexo = ''
                                    articulos= []

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
                                        # Buscar la sección de "Artículos"
                                        seccion_articulos = table_cvlac.find('h3', string='Artículos')
                                        if seccion_articulos:
                                            # Encontrar la fila (tr) siguiente después de la sección "Artículos"
                                            fila_articulos = seccion_articulos.find_parent('tr').find_next_sibling('tr')

                                            # Extraer los artículos de la segunda fila (tr)
                                            while fila_articulos:
                                                celdas_articulos = fila_articulos.find_all('td')
                                                
                                                #Dentro de blockquote es donde se encuentra toda la informacion del articulo
                                                if celdas_articulos:
                                                    elementos_blockquote = celdas_articulos[0].find('blockquote')
                                                    elementos_li = celdas_articulos[0].find_all('li')

                                                     # Obtener estado y tipo de articulo
                                                    if elementos_li:
                                                        for elemento in elementos_li:
                                                            texto_articulo = elemento.find('b').text.strip()
                                                            img_tag = elemento.find('img')
                                                            estado = 'Vigente' if img_tag else 'No Vigente'
                                                            tipo_articulo = texto_articulo.split(' - ')[-1]

                                                    #Obtener los datos del articulo
                                                    if elementos_blockquote:
                                                        texto_blockquote = elementos_blockquote.get_text(strip=True)
                                                        indice_comilla1 = texto_blockquote.find('"')
                                                        if indice_comilla1 != -1:
                                                            indice_comilla2 = texto_blockquote.find('"', indice_comilla1 + 1)
                                                            #Obtener titulo del articulo
                                                            if indice_comilla2 != -1:
                                                                titulo_articulo = texto_blockquote[indice_comilla1 + 1:indice_comilla2]
                                                                indice_pais = texto_blockquote.find(". En:")
                                                                #Obtener pais del articulo
                                                                if indice_pais != -1:
                                                                    indice_palabra_despues_de_en = indice_pais + len(". En:")
                                                                    palabra_despues_de_en = ""
                                                                    for i in range(indice_palabra_despues_de_en, len(texto_blockquote)):
                                                                        if texto_blockquote[i].isalpha() or texto_blockquote[i] == " ":
                                                                            palabra_despues_de_en += texto_blockquote[i]
                                                                        else:
                                                                            break
                                                                    pais = palabra_despues_de_en.strip()
                                                            
                                                                #Obtener ISSN del articulo
                                                                indice_issn = texto_blockquote.find("ISSN:")
                                                                indice_ed = texto_blockquote.find("ed", indice_issn)
                                                                indice_p = texto_blockquote.find("p.", indice_issn)
                                                                if indice_issn != -1:
                                                                    if indice_ed != -1 and indice_p != -1:
                                                                        if indice_ed < indice_p:
                                                                            issn = texto_blockquote[indice_issn + len("ISSN:"):indice_ed].strip()
                                                                        else:
                                                                            issn = texto_blockquote[indice_issn + len("ISSN:"):indice_p].strip()
                                                                    elif indice_ed != -1:
                                                                        issn = texto_blockquote[indice_issn + len("ISSN:"):indice_ed].strip()
                                                                    elif indice_p != -1:
                                                                        issn = texto_blockquote[indice_issn + len("ISSN:"):indice_p].strip()
                                                                    else:
                                                                        issn = texto_blockquote[indice_issn + len("ISSN:"):].strip()   
                                                                
                                                                #Obtener editorial del articulo
                                                                indice_ed = texto_blockquote.find("ed:", indice_issn)
                                                                indice_v = texto_blockquote.find("v.", indice_ed)
                                                                if indice_ed != -1:
                                                                    if indice_v != -1:
                                                                        editorial = texto_blockquote[indice_ed + len("ed:"):indice_v].strip()
                                                                    else:
                                                                        editorial = texto_blockquote[indice_ed + len("ed:"):].strip()
                                                                else:
                                                                    editorial = ""  # Deja la editorial en blanco si no se encuentra "ed:"
                                                                
                                                                #Obtener Volumen del articulo
                                                                indice_v = texto_blockquote.find("v.", indice_issn)
                                                                indice_fasc = texto_blockquote.find("fasc.", indice_v)
                                                                if indice_v != -1:
                                                                    if indice_fasc != -1:
                                                                        volumen = texto_blockquote[indice_v + len("v."):indice_fasc].strip()
                                                                    else:
                                                                        volumen = texto_blockquote[indice_v + len("v."):].strip()
                                                                    if not volumen.isdigit():
                                                                        volumen = ""
                                                                else:
                                                                    volumen = ""  # Deja el volumen en blanco si no se encuentra "v."
                                                                
                                                                #Obtener fasciculo del articulo
                                                                indice_fasc = texto_blockquote.find("fasc.", indice_v)
                                                                indice_p = texto_blockquote.find("p.", indice_fasc)
                                                                if indice_fasc != -1:
                                                                    if indice_p != -1:
                                                                        fasciculo = texto_blockquote[indice_fasc + len("fasc."):indice_p].strip()
                                                                    else:
                                                                        fasciculo = texto_blockquote[indice_fasc + len("fasc."):].strip()
                                                                else:
                                                                    fasciculo = ""  # Deja el fascículo en blanco si no se encuentra "fasc."
                                                                if fasciculo.isdigit():
                                                                    if len(fasciculo) == 4:
                                                                        fasciculo = ""
                                                                else:
                                                                    fasciculo = ""

                                                                #Obtener palabras claves del articulo
                                                                indice_palabras = texto_blockquote.find("Palabras:")
                                                                if indice_palabras != -1:
                                                                    indice_sectores = texto_blockquote.find("Sectores:", indice_palabras)
                                                                    if indice_sectores != -1:
                                                                        palabras_texto = texto_blockquote[indice_palabras + len("Palabras:"):indice_sectores].strip()
                                                                    else:
                                                                        palabras_texto = texto_blockquote[indice_palabras + len("Palabras:"):].strip()
                                                                    palabras = [palabra.strip() for palabra in palabras_texto.split(",") if palabra.strip()]
                                                                else:
                                                                    palabras = [] # Deja las palabras en blanco si no se encuentra "Palabras:"
                                                                
                                                                nombres_integrantes = texto_blockquote[:indice_comilla1].split(',')
                                                                nombres_integrantes = [nombre.strip() for nombre in nombres_integrantes if nombre.strip()] 
                                                                articulo_existente = next((a for a in articulos if a[0] == titulo_articulo), None) 
                                                                                                                              
                                                                if articulo_existente:
                                                                    # Si el artículo ya está en la lista, agregar los nuevos integrantes
                                                                    articulo_existente[1].extend([nombre for nombre in nombres_integrantes if nombre not in articulo_existente[1]])
                                                                else:
                                                                    # Si el artículo no está en la lista, agregarlo
                                                                    articulos.append((titulo_articulo, nombres_integrantes, tipo_articulo, estado, pais, issn, editorial, volumen, fasciculo, palabras))
                                                                   
                                                #Va al siguinete articulo                
                                                fila_articulos = fila_articulos.find_next_sibling('tr')
                                               
                                    # Agregar los datos del integrante y sus artículos a la lista
                                    integrantes.append([nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria, articulos])

                                except requests.exceptions.RequestException:
                                    # En caso de error en la solicitud HTTP
                                    integrantes.append(['', '', '', '', '', []])

            except requests.exceptions.RequestException:
                # En caso de error en la solicitud HTTP
                integrantes = []

            # Devolver los datos del grupo y sus integrantes
            return [nombre_grupo, enlace_gruplac_grupo, nombre_lider, cvlac_lider, integrantes]

    return []

#Proceso de agregar la infromación al CSV
try:
    # Realizar la solicitud HTTP
    response = session.get(url, verify=False)

    # Analizar el HTML
    soup = BeautifulSoup(response.text, 'lxml')

    # Encontrar y extraer la información deseada
    filas = soup.find_all('tr')[1:] # Omitir la primera fila que contiene la línea no deseada

    # Abrir el archivo de salida en modo de escritura
    with open(archivo_salida, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        #El nombre de las columnas en el csv
        writer.writerow(['Nombre del grupo', 'Enlace al GrupLac', 'Nombre del líder', 'Enlace al CvLac líder', 'Nombre del integrante', 'Enlace al CvLav del investigador', 'Nombre en citaciones', 'Nacionalidad', 'Sexo', 'Categoría', 'Título Artículo', 'Integrantes involucrados', 'Tipo Artículo', 'Estado', 'País', 'ISSN', 'Editorial', 'Volumen', 'Fascículo', 'Palabras clave'])

        # Crear hilos para procesar los grupos 
        with ThreadPoolExecutor() as executor:
            # Mapear la función procesar_grupo a cada fila de la tabla
            resultados = list(executor.map(procesar_grupo, filas))

            for datos in resultados:
                if datos:
                    grupo, enlace_grupo, lider, cvlac_lider, integrantes = datos
                    writer.writerow([grupo, enlace_grupo, lider, cvlac_lider, '', '', '', '', '', ''])
                    for integrante in integrantes:
                        nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria, articulos = integrante
                        writer.writerow(['', '', '', '', nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria, ''])
                        if articulos:
                            for articulo in articulos:
                                titulo_articulo,nombres_integrantes, tipo_articulo, estado, pais, issn, editorial, volumen,fasciculo, palabras = articulo
                                writer.writerow(['', '', '', '', '', '', '', '', '', titulo_articulo, nombres_integrantes, tipo_articulo, estado, pais, issn, editorial, volumen, fasciculo, palabras])
                        else:
                            writer.writerow(['', '', '', '', '', '', '', '', '', ''])

    print("Resultados almacenados en", archivo_salida)

except requests.exceptions.ConnectionError as e:
    print("Error de conexión:", e)