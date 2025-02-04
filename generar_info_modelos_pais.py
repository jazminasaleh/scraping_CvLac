import pandas as pd

# Cargar el archivo CSV original
ruta_archivo_original = "resultados_grupos_csv.csv"
df = pd.read_csv(ruta_archivo_original)

# Crear una función para determinar el área según la prioridad
def determinar_area(row):
    # Primero verifica Área general
    if pd.notna(row['Área general']) and str(row['Área general']).strip() != '':
        return row['Área general']
    
    # Si no hay Área general, verifica Area General Investigador
    elif pd.notna(row['Area General Investigador']) and str(row['Area General Investigador']).strip() != '':
        return row['Area General Investigador']
    
    # Si ninguna de las anteriores, usa Área de conocimiento general
    else:
        return row['Área de conocimiento general']

# Crear una nueva columna 'area' usando la función de priorización
df['area'] = df.apply(determinar_area, axis=1)

# Rellenar 'Año publicación' con valores de 'Fecha patente' si está en blanco
def rellenar_anio_publicacion(row):
    if pd.isna(row['Año publicación']) or str(row['Año publicación']).strip() == '':
        return row['Fecha patente']
    return row['Año publicación']

df['Año publicación'] = df.apply(rellenar_anio_publicacion, axis=1)

# Filtrar las columnas relevantes, usando la nueva columna 'area'
columnas_relevantes = ["area", "Año publicación", "País"]
df_filtrado = df[columnas_relevantes]

# Eliminar filas con valores faltantes
df_filtrado = df_filtrado.dropna()

# Asegurarse de que los años sean enteros
df_filtrado["Año publicación"] = df_filtrado["Año publicación"].astype(int)

# Agregar una columna auxiliar para contar las publicaciones
df_filtrado["# Publicaciones"] = 1

# Agrupar por la nueva columna area y Año, sumando las publicaciones
df_agrupado = df_filtrado.groupby(["area", "País", "Año publicación"], as_index=False).sum()

# Guardar el nuevo archivo CSV
ruta_archivo_nuevo = "areas_pais_anio.csv"
df_agrupado.to_csv(ruta_archivo_nuevo, index=False)
print(f"Archivo nuevo creado con conteo de publicaciones: {ruta_archivo_nuevo}")

# Para verificar la distribución de las fuentes de área
print("\nDistribución de las fuentes de área:")
area_sources = df.apply(lambda row: 
    'Área general' if pd.notna(row['Área general']) and str(row['Área general']).strip() != '' 
    else ('Area General Investigador' if pd.notna(row['Area General Investigador']) and str(row['Area General Investigador']).strip() != '' 
    else 'Área de conocimiento general'), axis=1)
print(area_sources.value_counts())