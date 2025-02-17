# Script de reprocesos

Este script permite procesar nuevamente los lanzamientos batch u ondemand de DEE. 

El script realiza durante su ejecución los siguientes pasos:

1. Consulta el estado DIAN de la base de datos de Efactura.
2. Valida el último estado insertado en la base de datos de 5X para cada registro.
3. Selecciona mensaje de reencolamiento según el último estado encontrado. Es importante tener en cuenta que los procesos batch se reencolarán desde el guardado de la respuesta DIAN.
4. Encola mensaje para iniciar el reproceso.

Los estados en la base de datos de 5X son:
- Estado 1: Finaliza mapeo del parser.
- Estado 2: Finaliza guardado del estado DIAN.
- Estado 3: Finaliza generación de JSON para composición.
- Estado 4: Finaliza composición de documento.
- Estado 5: Envia a integraciones digitales.
- Estado 12: Finaliza generación de JSON para la DIAN (Aplica solo para procesos batch)

## Requisitos

Asegúrate de tener instalado:

- Python 3.x ([Descargar aquí](https://www.python.org/downloads/))
- Dependencias del proyecto

## Instalación de dependencias

1. Ejecuta el siguiente comando para la instalación de dependencias 

    ```
    pip install -r requirements.txt
    ```

## Cómo ejecutar el script?

1. **Asegurate de tener el *.py* en la raiz** : El ejecutable debe estar en la raiz.
2. **Asegurate de tener configuradas todas las variables de entorno en el archivo *.env***: Configura todas las variables de entorno necesarias para la ejecución del script.
3. **Estructura de carpetas**: Para la correcta ejecución del script es necesario mantener la siguiente estructura de carpetas.
    ```
    Proyecto/
    │   .env
    │   README.md
    │   reencolarProcesosDEE.py
    │   requirements.txt
    │
    ├───error_csv
    ├───input_csv
    ├───output_data
    ├───processed_csv
    └───tmp
    ```
4. **Estructura del archivo de entrada**: Para la correcta ejecución del script, el archivo *.csv* de entrada debe tener la siguiente estructura **NIT_EMISOR;TIPO_DOCUMENTO;NUMERO_DOCUMENTO**. 
5. **Guarda el arhivo de entrada en la carpeta *input_csv***: El archivo que contiene los registros a reprocesar, debe ser almacenado en la carpeta *input_csv*.
6. **Configura el script antes de ejecutarlo**: En el metodo *__main__* agrega en las variables *file_name* y *has_header* el nombre del archivo a ejecutar y si tiene o no encabezado el archivo *.csv* respectivamente.
7. **Ejecuta el proyecto**: Para ejecutar el proyecto solo debes ejecutar el comando
    ```
    py reencolarProcesosDEE.py
    ```
