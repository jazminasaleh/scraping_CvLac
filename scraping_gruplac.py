import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from bs4 import BeautifulSoup
import csv
import os
## Obtener la infromación del Cvalca de los investigadores de la UPTC, por meido de Webscraping
## Programar una tarea automática para que se ejecute cada mes (falta)


# Desactivar las advertencias de solicitudes inseguras (solo para pruebas)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurar una estrategia de reintentos personalizada
retry_strategy = Retry(
    total=5,  # Número máximo de reintentos
    status_forcelist=[429, 500, 502, 503, 504],  # Códigos de estado para reintentar
    allowed_methods=["HEAD", "GET", "OPTIONS"],  # Métodos HTTP permitidos para reintentar
    backoff_factor=1  # Factor de retroceso para el tiempo de espera entre reintentos
)

# Crear una sesión personalizada con la estrategia de reintentos
session = requests.Session()
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Establecer el tiempo de espera de la sesión
session.timeout = 30  # Tiempo de espera de 30 segundos
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''

# URL del GrupLac, se toman los 152 grupos
url = 'https://scienti.minciencias.gov.co/ciencia-war/busquedaGrupoXInstitucionGrupos.do?codInst=930&sglPais=&sgDepartamento=&maxRows=152&grupos_tr_=true&grupos_p_=1&grupos_mr_=152'

# Los resultados se ven a almacenar en un csv con nombre resultados_grupos
archivo_salida = 'resultados_grupos.csv'

try:
    # Realizar la solicitud HTTP
    response = session.get(url, verify=False)

    # El código para procesar la respuesta
    # Analizar el HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    # Encontrar y extraer la información deseada
    filas = soup.find_all('tr')

    # Abrir el archivo de salida en modo de escritura
    with open(archivo_salida, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Nombre del grupo','GrupLac', 'Nombre del líder', 'CvLac líder', 'Nombre del intergrante', 'CvLav Integrante', 'Nombre en citaciones', 'Nacionalidad', 'Sexo'])

        # Iterar sobre cada fila de la tabla
        for fila in filas:
            # Encontrar todos los elementos <td> dentro de la fila
            columnas = fila.find_all('td')

            # Verificar si hay al menos tres columnas en la fila, ya que en la tercera columan es donde se encunetra en nombre del grupo
            if len(columnas) >= 3:
                # Obtener el tercer elemento <td>, en donde esta el nombre del grupo
                tercer_td = columnas[2]

                # Encontrar el primer elemento <a> dentro del tercer <td>
                enlace_grupo = tercer_td.find('a')
                # Verificar si se encontró un enlace dentro del tercer <td>
                if enlace_grupo:
                    # Extraer el texto y el enlace
                    nombre_grupo = enlace_grupo.text.strip()
                    href_enlace = enlace_grupo.get('href')
                    numero_url = href_enlace.split('=')[-1]
                    enlace_gruplac_grupo = f'https://scienti.minciencias.gov.co/gruplac/jsp/visualiza/visualizagr.jsp?nro={numero_url}'
                   

                    # Obtener el nombre del líder que se encuentra en la cuarta columna
                    nombre_lider = columnas[3].text.strip()
                    cvlac_lider = ''
                    # Encontrar el enlace del líder
                    enlace_lider = columnas[3].find('a')
                    if enlace_lider:
                        href_enlace_lider = enlace_lider.get('href')
                        numero_url_lider = href_enlace_lider.split('=')[-1]
                        cvlac_lider = f'https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh={numero_url_lider}'
                    
                    #Obtener integrantes de cada grupo y su información
                    try:
                       response_grupo = session.get(enlace_gruplac_grupo)
                       response_grupo.raise_for_status()
                       soup_grupo = BeautifulSoup(response_grupo.text, 'html.parser')

                        # Encontrar todas las tablas en la página
                       tables = soup_grupo.find_all('table')

                       for table in tables:
                            # Buscar el tbody en cada tabla
                                primer_tr = table.find('tr')
                                primer_td = primer_tr.find('td')
                                # Imprime el primer tr dentro del tbody
                                if primer_td and primer_td.text.strip() == "Integrantes del grupo":
                                    # Encontrado el encabezado "Integrantes del grupo" dentro del tbody
                                    # Buscar el tercer td dentro del tr
                                    filas_tabla = table.find_all('tr')[2:]  # Índice 2 para el tercer td
                                    
                                    for tercer_tr in filas_tabla:
                                        # Buscar todos los enlaces (a) dentro de la fila
                                        enlaces_integrantes = tercer_tr.find_all('a')
                                        # Iterar sobre los enlaces para obtener los nombres de los integrantes
                                        for enlace_integrante in enlaces_integrantes:
                                            # Extraer el texto del enlace (nombre del integrante)
                                            nombre_integrante = enlace_integrante.text.strip()
                                            enlace_cvlac_integrante = enlace_integrante.get('href')
                                            # Escribir el nombre del integrante en una fila separada en el archivo CSV
                                            try:
                                                response_cvlac_integrante = session.get(enlace_cvlac_integrante)
                                                response_cvlac_integrante.raise_for_status()
                                                soup_cvlac_integrante = BeautifulSoup(response_cvlac_integrante.text, 'html.parser')

                                                # Buscar todas las tablas en la página de CVLAC
                                                tables_cvlac = soup_cvlac_integrante.find_all('table')

                                                # Iterar sobre cada tabla en la página de CVLAC
                                                for table_cvlac in tables_cvlac:
                                                    nombre_citaciones_td = table_cvlac.find('td', string = 'Nombre en citaciones')
                                                     # Verificar si se encontró el td
                                                    if nombre_citaciones_td:
                                                        # Obtener el valor del nombre en citaciones
                                                        nombre_citaciones = nombre_citaciones_td.find_next('td').text.strip()
                                                        print(nombre_citaciones)
                                                         
                                                    # Buscar el td que contiene el texto "Nacionalidad"
                                                    
                                                    nacionalidad_td = table_cvlac.find('td', string = 'Nacionalidad')
                                                    if nacionalidad_td:
                                                        # Obtener el valor del nombre en citaciones
                                                        nacionalidad = nacionalidad_td.find_next('td').text.strip()
                                                        print(nacionalidad)
                                                         
                                                    
                                                    sexo_td = table_cvlac.find('td', string='Sexo')
                                                    # Verificar si se encontró el td
                                                    if sexo_td:
                                                        # Obtener el valor del sexo
                                                        sexo = sexo_td.find_next('td').text.strip()
                                                        print(sexo)
                                                        break  

                                            except requests.exceptions.RequestException:
                                                # En caso de error en la solicitud HTTP
                                                sexo = '', 
                                                nombre_citaciones = '',
                                                nacionalidad = ''
                                            # Aca se coloca la información que se piensa sacar del CvLac, digamos articulos, idioma....

                                            # Escribir los datos en el archivo CSV
                                            writer.writerow([nombre_grupo, enlace_gruplac_grupo, nombre_lider, cvlac_lider, nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo])
                           
                    except:
                        nombre_integrante = '',
                        enlace_cvlac_integrante = ''

    print("Resultados almacenados en", archivo_salida)

except requests.exceptions.ConnectionError as e:
    print("Error de conexión:", e)
