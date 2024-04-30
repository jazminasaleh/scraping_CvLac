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

# Los resultados se ven a almacenar en un csv con nombre resultados_grupos
archivo_salida = 'resultados_grupos.csv'

# Función para procesar un grupo
def procesar_grupo(fila):
    columnas = fila.find_all('td')

    # Verificar si hay al menos tres columnas en la fila
    if len(columnas) >= 3:
        tercer_td = columnas[2]
        enlace_grupo = tercer_td.find('a')

        # Verificar si se encontró un enlace dentro del tercer <td>
        if enlace_grupo:
            # Extraer el texto y el enlace
            nombre_grupo = enlace_grupo.text.strip()
            href_enlace = enlace_grupo.get('href')
            numero_url = href_enlace.split('=')[-1]
            enlace_gruplac_grupo = f'https://scienti.minciencias.gov.co/gruplac/jsp/visualiza/visualizagr.jsp?nro={numero_url}'

            # Obtener el nombre del líder
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
                                    nacionalidad = ''
                                    sexo = ''

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

                                    # Agregar los datos del integrante a la lista
                                    integrantes.append([nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo])

                                except requests.exceptions.RequestException:
                                    # En caso de error en la solicitud HTTP
                                    integrantes.append(['', '', '', '', ''])

            except requests.exceptions.RequestException:
                # En caso de error en la solicitud HTTP
                integrantes = []

            # Devolver los datos del grupo y sus integrantes
            return [nombre_grupo, enlace_gruplac_grupo, nombre_lider, cvlac_lider, integrantes]

    return []

try:
    # Realizar la solicitud HTTP
    response = session.get(url, verify=False)

    # Analizar el HTML
    soup = BeautifulSoup(response.text, 'lxml')

    # Encontrar y extraer la información deseada
    filas = soup.find_all('tr')

    # Abrir el archivo de salida en modo de escritura
    with open(archivo_salida, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Nombre del grupo', 'GrupLac', 'Nombre del líder', 'CvLac líder', 'Nombre del integrante', 'CvLav Integrante', 'Nombre en citaciones', 'Nacionalidad', 'Sexo'])

        # Crear un pool de hilos para procesar los grupos en paralelo
        with ThreadPoolExecutor() as executor:
            # Mapear la función procesar_grupo a cada fila de la tabla
            resultados = list(executor.map(procesar_grupo, filas))

            for datos in resultados:
                if datos:
                    grupo, enlace_grupo, lider, cvlac_lider, integrantes = datos
                    writer.writerow([grupo, enlace_grupo, lider, cvlac_lider, '', '', '', '', ''])
                    for integrante in integrantes:
                        nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo = integrante
                        writer.writerow(['', '', '', '', nombre_integrante, enlace_cvlac_integrante, nombre_citaciones, nacionalidad, sexo])

    print("Resultados almacenados en", archivo_salida)

except requests.exceptions.ConnectionError as e:
    print("Error de conexión:", e)