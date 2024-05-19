import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from bs4 import BeautifulSoup, SoupStrainer
import csv
import os
from concurrent.futures import ThreadPoolExecutor
import re

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

paises_espanol = [
    "Afganistán", "Albania", "Alemania", "Andorra", "Angola", "Antigua y Barbuda", "Arabia Saudita", "Argelia",
    "Argentina", "Armenia", "Australia", "Austria", "Azerbaiyán", "Bahamas", "Bangladés", "Barbados", "Baréin",
    "Bélgica", "Belice", "Benín", "Bielorrusia", "Birmania", "Bolivia", "Bosnia y Herzegovina", "Botsuana",
    "Brasil", "Brunéi", "Bulgaria", "Burkina Faso", "Burundi", "Bután", "Cabo Verde", "Camboya", "Camerún",
    "Canadá", "Catar", "Chad", "Chile", "China", "Chipre", "Ciudad del Vaticano", "Colombia", "Comoras", "Corea del Norte",
    "Corea del Sur", "Costa de Marfil", "Costa Rica", "Croacia", "Cuba", "Dinamarca", "Dominica", "Ecuador", "Egipto",
    "El Salvador", "Emiratos Árabes Unidos", "Eritrea", "Eslovaquia", "Eslovenia", "España", "Estados Unidos",
    "Estonia", "Etiopía", "Filipinas", "Finlandia", "Fiyi", "Francia", "Gabón", "Gambia", "Georgia", "Ghana",
    "Granada", "Grecia", "Guatemala", "Guyana", "Guinea", "Guinea ecuatorial", "Guinea-Bisáu", "Haití", "Honduras",
    "Hungría", "India", "Indonesia", "Irak", "Irán", "Irlanda", "Islandia", "Islas Marshall", "Islas Salomón",
    "Israel", "Italia", "Jamaica", "Japón", "Jordania", "Kazajistán", "Kenia", "Kirguistán", "Kiribati", "Kuwait",
    "Laos", "Lesoto", "Letonia", "Líbano", "Liberia", "Libia", "Liechtenstein", "Lituania", "Luxemburgo", "Macedonia del Norte",
    "Madagascar", "Malasia", "Malaui", "Maldivas", "Malí", "Malta", "Marruecos", "Mauricio", "Mauritania", "México",
    "Micronesia", "Moldavia", "Mónaco", "Mongolia", "Montenegro", "Mozambique", "Namibia", "Nauru", "Nepal",
    "Nicaragua", "Níger", "Nigeria", "Noruega", "Nueva Zelanda", "Omán", "Países Bajos", "Pakistán", "Palaos",
    "Panamá", "Papúa Nueva Guinea", "Paraguay", "Perú", "Polonia", "Portugal", "Reino Unido", "República Centroafricana",
    "República Checa", "República del Congo", "República Democrática del Congo", "República Dominicana",
    "República Sudafricana", "Ruanda", "Rumania", "Rusia", "Samoa", "San Cristóbal y Nieves", "San Marino",
    "San Vicente y las Granadinas", "Santa Lucía", "Santo Tomé y Príncipe", "Senegal", "Serbia", "Seychelles",
    "Sierra Leona", "Singapur", "Siria", "Somalia", "Sri Lanka", "Suazilandia", "Sudán", "Sudán del Sur", "Suecia",
    "Suiza", "Surinam", "Tailandia", "Tanzania", "Tayikistán", "Timor Oriental", "Togo", "Tonga", "Trinidad y Tobago",
    "Túnez", "Turkmenistán", "Turquía", "Tuvalu", "Ucrania", "Uganda", "Uruguay", "Uzbekistán", "Vanuatu",
    "Venezuela", "Vietnam", "Yemen", "Yibuti", "Zambia", "Zimbabue"
]

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
                                        seccion_articulos = table_cvlac.find('h3', string=['Artículos','Libros','Capitulos de libro', 'Textos en publicaciones no científicas'])
                                        if seccion_articulos:
                                            tipo_producto = seccion_articulos.text
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
                                                            texto_articulo=elemento.find('b')
                                                            tipo_articulo = ''
                                                            if texto_articulo:
                                                                tipo_articulo = texto_articulo.get_text().split(' - ')[-1]
                                                            else:
                                                                tipo_articulo = "Capítulo de Libro"
                                                            img_tag = elemento.find('img')
                                                            estado = 'Vigente' if img_tag else 'No Vigente'
                                                    #Obtener los datos del articulo
                                                    titulo_revista = ""
                                                    if elementos_blockquote:
                                                        texto_blockquote = elementos_blockquote.get_text(strip=True)
                                                        texto_blockquote = " ".join(texto_blockquote.split()) 
                                                       
                                                        indice_comilla1 = texto_blockquote.find('"')
                                                        if indice_comilla1 != -1:
                                                            indice_comilla2 = texto_blockquote.find('"', indice_comilla1 + 1)
                                                            #Obtener titulo del articulo
                                                            if indice_comilla2 != -1:
                                                                titulo_articulo = texto_blockquote[indice_comilla1 + 1:indice_comilla2]
                                                                tipo_articulo = tipo_articulo.title()
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
                                                                    # Validar si las primeras letras coinciden con un país
                                                                    #Obtener el pais
                                                                    paises_validos = [pais.lower() for pais in paises_espanol]
                                                                    pais_encontrado = False
                                                                    titulo_revista = ""
                                                                    for pais_valido in paises_validos:
                                                                        nombre_pais = " ".join(palabras[:3]).lower()
                                                                        if nombre_pais == pais_valido or nombre_pais.startswith(pais_valido + " ") or nombre_pais.startswith(pais_valido):
                                                                            pais = pais_valido.capitalize()
                                                                            #Obtener el titulo de la revista donde se público el articulo
                                                                            if len(pais) > 1 and tipo_producto == "Artículos":
                                                                                indice_final_pais = texto_blockquote.find("En:") + len(pais) + 4
                                                                                indice_issn = texto_blockquote.find("ISSN:")
                                                                                texto_despues_pais = texto_blockquote[indice_final_pais:indice_issn]
                                                                                if len(texto_despues_pais) > 1:
                                                                                    titulo_revista = texto_despues_pais
                                                                                else:
                                                                                    titulo_revista = ""
                                                                            elif len(pais) > 1 and tipo_producto == "Textos en publicaciones no científicas":
                                                                                indice_final_pais = texto_blockquote.find("En:") + len(pais) + 12
                                                                                indice_issn = texto_blockquote.find("ISSN:") -1
                                                                                if indice_issn != -1:
                                                                                    titulo_revista = texto_blockquote[indice_final_pais:indice_issn].strip()
                                                                                else:
                                                                                    titulo_revista = ""
                                                                            else:
                                                                                titulo_revista = ""
                                                                            pais_encontrado = True
                                                                            break
                                                                    if not pais_encontrado:
                                                                        pais = ""
                                                                    
                                                                issn = ''
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
                                                                indice_palabras_clave = texto_blockquote.find("Palabras:", indice_v)
                                                                if indice_v != -1:
                                                                    if indice_fasc != -1:
                                                                        volumen = texto_blockquote[indice_v + len("v."):indice_fasc].strip()
                                                                    elif indice_palabras_clave != -1:
                                                                        volumen = texto_blockquote[indice_v + len("v."):indice_palabras_clave].strip()
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

                                                                # Obtener el número de pagina
                                                                # Expresión regular para buscar el número de página
                                                                patron_pagina = r'(?:pages?|p\.)\s*(\d+)\s*-\s*(\d+)'

                                                                # Buscar la coincidencia en el texto
                                                                resultado_pagina = re.search(patron_pagina, texto_blockquote)

                                                                # Verificar si se encontró una coincidencia y extraer el número de página
                                                                if resultado_pagina:
                                                                    numero_pagina_inicio = resultado_pagina.group(1)
                                                                    numero_pagina_final = resultado_pagina.group(2)
                                                                    paginas = numero_pagina_inicio + '-' + numero_pagina_final
                                                                else:
                                                                    paginas = ""

                                                                # Obtener año de publicación
                                                                indices_comas = [m.start() for m in re.finditer(r',', texto_blockquote)]
                                                                indices_puntos = [m.start() for m in re.finditer(r'. ', texto_blockquote)]
                                                                año = ""

                                                                for i in range(len(indices_comas) - 1):
                                                                    posible_año = texto_blockquote[indices_comas[i] + 1:indices_comas[i + 1]].strip()
                                                                    if posible_año.isdigit() and len(posible_año) == 4:
                                                                        año = posible_año
                                                                        break

                                                                if not año:
                                                                    for indice_punto in indices_puntos:
                                                                        posible_año = texto_blockquote[indice_punto + 2:indice_punto + 6]
                                                                        if posible_año.isdigit():
                                                                            año = posible_año
                                                                            break
                                                                
                                                                #Obtener el DOI
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
                                                                else:
                                                                    doi = ""

                                                                #Obtener palabras claves del articulo
                                                                indice_palabras = texto_blockquote.find("Palabras:")
                                                                if indice_palabras != -1:
                                                                    indice_sectores = texto_blockquote.find("Sectores:", indice_palabras)
                                                                    if indice_sectores != -1:
                                                                        palabras_texto = texto_blockquote[indice_palabras + len("Palabras:"):indice_sectores].strip()
                                                                    else:
                                                                        palabras_texto = texto_blockquote[indice_palabras + len("Palabras:"):].strip()
                                                                    palabras_limpio = [palabra.strip() for palabra in palabras_texto.split(",") if palabra.strip()]
                                                                    palabras = ', '.join(palabras_limpio)
                                                                else:
                                                                    palabras = "" # Deja las palabras en blanco si no se encuentra "Palabras:"
                                                                
                                                                #Obtener areas
                                                                indice_areas = texto_blockquote.find("Areas:")
                                                                if indice_areas != -1:
                                                                    indice_sectores = texto_blockquote.find("Sectores:", indice_areas)
                                                                    if indice_sectores != -1:
                                                                        areas_texto = texto_blockquote[indice_areas + len("Areas:"):indice_sectores].strip()
                                                                    else:
                                                                        areas_texto = texto_blockquote[indice_areas + len("Areas:"):].strip()
                                                                    areas_limpio = [area.strip() for area in areas_texto.split(",") if area.strip()]
                                                                    areas = ', '.join(areas_limpio)
                                                                else:
                                                                    areas = "" # Deja las áreas en blanco si no se encuentra "Areas:"
                                                                

                                                                # Obtener sectores del artículo
                                                                indice_sectores = texto_blockquote.find("Sectores:")
                                                                if indice_sectores != -1:
                                                                    sectores_texto = texto_blockquote[indice_sectores + len("Sectores:"):].strip()
                                                                     # Limpiar y dividir los sectores
                                                                    sectores_limpio = [sector.strip() for sector in sectores_texto.split(",") if sector.strip()]
                                                                    sectores = ', '.join(sectores_limpio)
                                                                else:
                                                                    sectores = "" # Deja los sectores en blanco si no se encuentra "Sectores:"

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
                                                                            #Solo deja la primera letra en mayuscula
                                                                            nombre_limpio = nombre_limpio.title()
                                                                            nombres_integrantes_limpios.append(nombre_limpio)
                                                                        continue
                                                                    
                                                                    nombre_limpio = re.sub(r"['\\]", '', nombre[:indice_tipo].strip())
                                                                    nombres_integrantes_limpios.append(nombre_limpio)
                                                                    

                                                                nombres_integrantes_str = ', '.join(nombres_integrantes_limpios)
                                                                nombres_integrantes_lista = nombres_integrantes_str.split(',')
                                                                
                                                               
                                                                articulo_existente = next((a for a in articulos if a[0] == titulo_articulo), None)
                                                                                                                              
                                                                if articulo_existente is not None:
                                                                    if isinstance(articulo_existente[1], list):
                                                                        articulo_existente[1].extend([nombre for nombre in nombres_integrantes_lista if nombre.strip() and nombre.strip() not in articulo_existente[1]])
                                                                    else:
                                                                        # Si articulo_existente[1] no es una lista, puedes crear una nueva lista con los nombres anteriores y los nuevos
                                                                        articulo_existente = list(articulo_existente)
                                                                        articulo_existente[1] = articulo_existente[1].split(', ') + [nombre for nombre in nombres_integrantes_lista if nombre.strip() and nombre.strip() not in articulo_existente[1]]
                                                                else:
                                                                    # Si el artículo no está en la lista, agregarlo
                                                                    articulos.append((titulo_articulo, nombres_integrantes_str,  tipo_producto, tipo_articulo, estado, pais, titulo_revista, issn, editorial, volumen, fasciculo, paginas, año, doi, palabras, areas, sectores))
                                                                   
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
    soup = BeautifulSoup(response.text, 'html.parser', from_encoding='utf-8')

    # Encontrar y extraer la información deseada
    filas = soup.find_all('tr')[1:] # Omitir la primera fila que contiene la línea no deseada

    # Abrir el archivo de salida en modo de escritura
    with open(archivo_salida, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # El nombre de las columnas en el CSV
        writer.writerow(['Nombre del grupo', 'Enlace al GrupLac', 'Nombre del líder', 'Enlace al CvLac líder', 'Nombre del integrante', 'Enlace al CvLac del investigador', 'Nombre en citaciones', 'Nacionalidad', 'Sexo', 'Categoría', 'Título publicación', 'Integrantes involucrados', 'Tipo producto', 'Tipo publicación', 'Estado', 'País','Titulo revista','ISSN', 'Editorial', 'Volumen', 'Fascículo', 'Páginas', 'Año publicación', 'DOI', 'Palabras clave', 'Areas', 'Sectores'])

        # Crear hilos para procesar los grupos
        with ThreadPoolExecutor() as executor:
            # Mapear la función procesar_grupo a cada fila de la tabla
            resultados = list(executor.map(procesar_grupo, filas))

            for datos in resultados:
                if datos:
                    grupo, enlace_grupo, lider, cvlac_lider, integrantes = datos
                    for integrante in integrantes:
                        if len(integrante) == 7:
                            nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria, articulos = integrante
                            for articulo in articulos:
                                titulo_articulo, nombres_integrantes, tipo_producto, tipo_articulo, estado, pais,titulo_revista, issn, editorial, volumen, fasciculo, paginas, año, doi, palabras, areas, sectores = articulo
                                writer.writerow([grupo, enlace_grupo, lider, cvlac_lider, nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo, categoria, titulo_articulo, nombres_integrantes, tipo_producto, tipo_articulo, estado, pais, titulo_revista, issn, editorial, volumen, fasciculo, paginas,año, doi, palabras, areas, sectores])
    
    print("Resultados almacenados en", archivo_salida)

except requests.exceptions.ConnectionError as e:
    print("Error de conexión:", e)