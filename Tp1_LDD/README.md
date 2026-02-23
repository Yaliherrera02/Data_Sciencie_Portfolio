# Análisis de la Liga Belga: Rendimiento y Dinámica Competitiva (2009-2013)
📋 Sobre este proyecto
El objetivo de este proyecto es analizar el desempeño y la dinámica competitiva de la liga de fútbol de Bélgica a lo largo de cuatro temporadas consecutivas (2009/2010 - 2012/2013). A través de este estudio, busqué identificar patrones de éxito, evaluar el impacto de la localía y explorar la relación entre los atributos técnicos de los planteles y su capacidad goleadora.

Evolución del trabajo: De un TP grupal a una versión optimizada
Este proyecto surgió originalmente como un trabajo práctico para la materia Laboratorio de Datos (UBA) realizado en grupo. Luego de la entrega, realicé una refactorización individual con el fin de aplicar mejores prácticas de programación y optimizar el procesamiento de los datos.

¿Qué mejoras implementé en esta nueva versión?

Nombres declarativos: Reemplacé variables y tablas que tenían nombres genéricos por nombres que describen claramente su contenido. Esto hace que el código sea profesional y fácil de entender para cualquiera.


Gran reducción de código: Logré simplificar el script analisis.py en un 50%, pasando de unas 1700 líneas originales a aproximadamente 850. Eliminé procesos redundantes y consolidé la lógica para que sea más directa.

Procesamiento eficiente (DuckDB + DataFrames): En lugar de generar y leer archivos CSV constantemente, utilicé DuckDB para procesar los datos en memoria y transformarlos directamente en DataFrames de Pandas. Esto acelera la ejecución y mantiene limpio el espacio de trabajo.

Cálculos automáticos: El código ahora cuenta los partidos y calcula los promedios desde la base de datos real mediante SQL (COUNT, SUM), asegurando resultados exactos.


Estructura dinámica: El script identifica automáticamente los equipos y temporadas, lo que permite reutilizar el análisis con otros datos sin tener que modificar el código manualmente.


🛠️ Herramientas utilizadas

Python: Para la lógica de control y automatización.
DuckDB (SQL): Para realizar consultas complejas de forma eficiente.
Matplotlib y Seaborn: Para la creación de las visualizaciones.
Pandas: Para la manipulación y organización de tablas de datos.


📊 Análisis realizados

Evolución del rendimiento: Seguimiento de goles a favor y en contra por temporada.
Efectividad goleadora: Cálculo de promedios de gol por equipo según su permanencia en la liga.
Impacto de la localía: Comparativa del desempeño de los equipos jugando en casa versus como visitante.
Relación técnica: Análisis de la suma de atributos de los planteles en relación con su capacidad goleadora.


Este proyecto refleja mi proceso de aprendizaje en Ciencia de Datos, buscando siempre pasar de un código funcional a uno eficiente, legible y escalable. 

🚀 Cómo ejecutar el proyecto
Para replicar el análisis desde cero, seguí estos pasos en orden:
1. Clonar el repositorio:
Descargá los archivos en tu carpeta local.

2. Configurar el entorno virtual (Recomendado):
Para mantener las dependencias aisladas, creá y activá un entorno virtual:

Windows:
Bash
python -m venv .venv
.venv\Scripts\activate

macOS/Linux:
Bash
python3 -m venv .venv
source .venv/bin/activate

3. Instalar dependencias:
Con el entorno activado, instalá las librerías necesarias:
pip install -r requirements.txt

4. Generar el modelo relacional:
Ejecutá el script de procesamiento para transformar los datos crudos en tablas normalizadas. Este paso aplica las Dependencias Funcionales y las formas normales (1FN, 2FN, 3FN) definidas en el diseño:
python generar_tablas.py

5. Correr el análisis:
Una vez creadas las tablas, ejecutá el script principal que realiza las consultas SQL mediante DuckDB y genera las visualizaciones de performance:
python analisis.py

📂 Acceso a los Datos
Debido al tamaño de los archivos originales, el dataset completo se encuentra alojado en Google Drive.

Descargá la carpeta de datos desde: https://drive.google.com/drive/folders/15zsboEZP0iqAG1T51ydxrwC_yXrwAV-I?usp=sharing

Colocá la carpeta descargada con el nombre enunciado_tablas en la raíz del proyecto para poder ejecutar los scripts.
