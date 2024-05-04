# scraping_CvLac

El objetivo es extraer la información de los investigadores de la Univeridad Pedagógica y Tecnólogica de Colombia (Uptc), para el logro de este se utilizó la tecnica de web scraping.
Para poder obtener la información de todos los investigadores de la UPTC se pasaron por los siguientes pasos:

## Extraer la información del GrupLac y CvLac:
- Nombre del grupo de investigación
- Enlace al GrupLac del grupo
    - Nombre del líder del grupo
    - Enlace al CvLac del líder del grupo
    - Integrantes del grupo, con la siguiente información para cada uno:
        - Nombre
        - Enlace al CvLac de investigador
        - Nombre en citaciones
        - Nacionalidad
        - Sexo
        - Categoría
        - Artículos publicados por el investigador:
            - Título del artículo
            - Integrantes involucrados
            - Tipo Artículo: especifica el tipo de artículo en función de su extensión o alcance.
            - Estado del artículo (vigente o no vigente)
            - País de publicación
            - ISSN (Número Internacional Normalizado de Publicaciones Seriadas): Es un identificador único para publicaciones periódicas como revistas y periódicos.
            - Editorial: Indica la institución o editorial responsable de la publicación del artículo.
            - Volumen: Se refiere al número de volumen de la revista en la que se publicó el artículo.
            - Fascículo: es una división de una revista que agrupa varios artículos relacionados.
            - Palabras clave

### Ejemplo de hoja de vida privada:
[Ver hoja de vida](https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh=0001435492)
