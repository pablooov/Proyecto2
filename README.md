# Proyecto2 

#  Modelo Dimensional de Datos de Viajes 

Este repositorio contiene las consultas SQL necesarias para crear el modelo dimensional basado en los datos de viajes.

---

## Creación de Tablas

###  Tabla de Tiempo 
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
###TABLA UBICACIONES
```
CREATE TABLE dim_ubicaciones (
    location_id SERIAL PRIMARY KEY,
    pickup_location_id INT NOT NULL,
    dropoff_location_id INT NOT NULL
);
```
###TABLA BASES OPERATIVAS
```
CREATE TABLE dim_bases (
    base_id SERIAL PRIMARY KEY,
    base_name VARCHAR(100) NOT NULL,
    associated_company VARCHAR(100),
    service_type VARCHAR(50)
);

```
###TABLA VEHICULOS
```
CREATE TABLE dim_vehiculos (
    vehicle_id SERIAL PRIMARY KEY,
    hvfhs_license_num VARCHAR(50) NOT NULL,
    dispatching_base_num VARCHAR(50),
    originating_base_num VARCHAR(50)
);
```
###TABLA HECHOS DE VIAJE
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
###TABLA HECHOS DE INGRESO
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

### Cantidad de viajes por día
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

