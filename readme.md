#                       **PROYECTO INDIVIDUAL Nº1**



# Machine Learning Operations (MLOps)

Este proyecto es un sistema de exploración de datos basado en la plataforma de videojuegos Steam. Toma la información de un juego para identifica juegos similares, además, tiene funciones para los endpoints que se consumirán en la API. El proyecto posee un notebook con el ETL y la creación de las funciones para los endpoints que usaran las apis, tambien pose un main.py con la información de las apis y una carpeta data con los parquet.

## Preprocesamiento de Datos

Los datos provienen de [PI MLOps - STEAM - Google Drive](https://drive.google.com/drive/folders/1HqBG2-sUkz_R3h1dZU5F2uAzpRn7BSpj), son 3 dataset, uno con información de las reviews, otro de items y otros con los datos de los juegos de steam.

Para la limpieza de datos se desanidaron varias columnas con diccionarios, se eliminaron las columnas innecesarios, se cambiaron los tipos de datos en algunas columnas y en el df_reviews se elimino la columna reviews y se creo una nueva con un análisis de sentimiento para ver si las reseñas eran negativas positivas y neutrales. Los dataset modificados son los siguientes. (https://github.com/TomasBasovich/PIMLOPS-Steam)



## Exploración de Datos

El sistema se basa en el ingreso de un valor (dependiendo la función) el cual nos mostrará en pantalla lo requerido; ya sea el mejor desarrollador por año, el usuario que más dinero gastó y su porcentaje de recomendaciones, o el porcentaje de juegos gratuitos general y por año de cada desarrollador.

# API de Exploración de Datos en Steam

Esta API proporciona datos de usuarios y desarrolladores en la plataforma Steam basadas en el comportamiento de los usuarios y los datos de los juegos disponibles en Steam.

## Cómo Funciona

La API utiliza un algoritmo de filtrado colaborativo. A continuación, se describe el proceso general:

1. **Entrada del Usuario**: El usuario proporciona el ID de un usuario como parámetro en la URL al hacer una solicitud GET a ("/Userdata").

2. **Obtención de Datos del Usuario de Referencia**: La API obtiene los datos del usuario de referencia con el ID proporcionado por el usuario desde unos archivos parquet que contienen información sobre el precio de los juegos y usuarios de Steam (en este caso sus recomendaciones).

3. **Salida del Usuario**: La API identifica al usuario y muestra la cantidad de dinero que gastó, el porcentaje de recomendación y la cantidad de items.



## En resemen

- Devuelve la cantidad de dinero gastado.

- El porcentaje de juegos recomendados.

- La cantidad de items.

  

## Links

- Repositorio (Github): [github TomasBasovich/PIMLOPS-Steam](https://github.com/TomasBasovich/PIMLOPS-Steam)
- Video (Youtube):https://youtu.be/CXa0gQKn4FE