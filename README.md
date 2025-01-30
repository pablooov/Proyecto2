# Proyecto2 

#  Modelo Dimensional de Datos de Viajes 

Este repositorio contiene las consultas SQL necesarias para crear el modelo dimensional basado en los datos de viajes. Ademas y código Python para analizar un dataset de viajes.

---

## Creación de Tablas

Tabla de Tiempo 
```
CREATE TABLE dim_tiempo (
    time_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week VARCHAR(15) NOT NULL,
    hour INT NOT NULL
);
```
Tabla ubicaciones
```
CREATE TABLE dim_ubicaciones (
    location_id SERIAL PRIMARY KEY,
    pickup_location_id INT NOT NULL,
    dropoff_location_id INT NOT NULL
);
```
Tabla bases operativas
```
CREATE TABLE dim_bases (
    base_id SERIAL PRIMARY KEY,
    base_name VARCHAR(100) NOT NULL,
    associated_company VARCHAR(100),
    service_type VARCHAR(50)
);

```
Tabla Vehiculos
```
CREATE TABLE dim_vehiculos (
    vehicle_id SERIAL PRIMARY KEY,
    hvfhs_license_num VARCHAR(50) NOT NULL,
    dispatching_base_num VARCHAR(50),
    originating_base_num VARCHAR(50)
);
```
Tabla hechos de viaje
```
CREATE TABLE fact_viajes (
    trip_id SERIAL PRIMARY KEY,
    time_id INT REFERENCES dim_tiempo(time_id),
    location_id INT REFERENCES dim_ubicaciones(location_id),
    vehicle_id INT REFERENCES dim_vehiculos(vehicle_id),
    base_id INT REFERENCES dim_bases(base_id),
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    trip_miles FLOAT,
    trip_time INT
);
```
Tabla hechos ingreso
```
CREATE TABLE fact_ingresos (
    revenue_id SERIAL PRIMARY KEY,
    trip_id INT REFERENCES fact_viajes(trip_id),
    base_passenger_fare FLOAT,
    tolls FLOAT,
    sales_tax FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    tips FLOAT,
    driver_pay FLOAT
);

```
##  Consultas SQL 

 Cantidad de viajes por día
```
SELECT DATE(pickup_datetime) AS fecha, COUNT(*) AS cantidad_viajes
FROM fact_viajes
GROUP BY fecha
ORDER BY fecha;
`````
Consulta Ingresos totales por mes
```
SELECT DATE_TRUNC('month', pickup_datetime) AS mes, 
       SUM(base_passenger_fare + tolls + sales_tax + 
           congestion_surcharge + airport_fee + tips + driver_pay) AS ingresos_totales
FROM fact_ingresos
JOIN fact_viajes ON fact_ingresos.trip_id = fact_viajes.trip_id
GROUP BY mes
ORDER BY mes;
```
Consulta Ingresos totales por día
```
SELECT DATE(pickup_datetime) AS fecha, 
       SUM(base_passenger_fare + tolls + sales_tax + 
           congestion_surcharge + airport_fee + tips + driver_pay) AS ingresos_totales
FROM fact_ingresos
JOIN fact_viajes ON fact_ingresos.trip_id = fact_viajes.trip_id
GROUP BY fecha
ORDER BY fecha;
```
Consulta Top 10 días con más viajes
```
SELECT DATE(pickup_datetime) AS fecha, COUNT(*) AS cantidad_viajes
FROM fact_viajes
GROUP BY fecha
ORDER BY cantidad_viajes DESC
LIMIT 10;
```
Consulta Top 5 de horas con más ingresos
```

SELECT EXTRACT(HOUR FROM pickup_datetime) AS hora, 
       SUM(base_passenger_fare + tolls + sales_tax + 
           congestion_surcharge + airport_fee + tips + driver_pay) AS ingresos_totales
FROM fact_ingresos
JOIN fact_viajes ON fact_ingresos.trip_id = fact_viajes.trip_id
GROUP BY hora
ORDER BY ingresos_totales DESC
LIMIT 5;


```
Código Python para Análisis de Datos
```
import pyarrow.parquet as pq
import pandas as pd

# Configurar Pandas para mostrar todas las columnas sin truncar
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Ruta del archivo Parquet
archivo_parquet = "fhvhv_tripdata_2022-01.parquet"

# Leer el archivo Parquet
print("Cargando archivo Parquet...")
tabla = pq.read_table(archivo_parquet)

# Convertir a DataFrame de Pandas
df = tabla.to_pandas()
print(f" Archivo cargado correctamente con {df.shape[0]} registros y {df.shape[1]} columnas.\n")

# Mostrar los primeros 5 registros
print("Primeros 5 registros del archivo:")
print(df.head(), "\n")

# Contar el número de registros
num_registros = df.shape[0]
print(f" El archivo Parquet tiene {num_registros} registros.\n")

# Mostrar solo el primer registro con formato completo
print("Primer registro del archivo:")
print(df.head(1), "\n")

# Contar valores nulos por columna
valores_nulos = df.isnull().sum()
columnas_con_nulos = valores_nulos[valores_nulos > 0]

# Verificar si hay columnas con valores nulos
if columnas_con_nulos.empty:
    print(" No hay columnas con valores nulos.\n")
else:
    print(" Columnas con valores nulos:")
    print(columnas_con_nulos, "\n")

# Verificar y Mostrar algunos registros con valores nulos
filas_nulas = df[df.isnull().any(axis=1)]

if filas_nulas.empty:
    print(" No hay registros con valores nulos.\n")
else:
    print(" Ejemplo de registros con valores nulos:")
    print(filas_nulas.head(), "\n")

# Eliminar registros con valores nulos
df_limpio = df.dropna()
print(f" Filas antes: {len(df)}, después de eliminar nulos: {len(df_limpio)}\n")
