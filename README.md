# ETL-y-An-lisis-de-Retrasos-de-Vuelos-con-PySpark-en-Databricks
Diseñé y documenté un pipeline ETL en Databricks utilizando PySpark. Incluye limpieza de datos, enriquecimiento de información, análisis exploratorio, visualización de resultados y carga final en formato Parquet.
# ✈️ Análisis de Retrasos de Vuelos con PySpark en Databricks

Este proyecto implementa un pipeline **ETL (Extract, Transform, Load)** en **Databricks** utilizando **PySpark** para analizar retrasos de vuelos en EE.UU. El objetivo es demostrar habilidades prácticas en procesamiento de datos distribuidos, análisis exploratorio y visualización de resultados, presentando un caso realista para un portafolio de **Data Engineer** o **Data Scientist**.

---

## 📂 Dataset
- Fuente: [`/databricks-datasets/learning-spark-v2/flights/departuredelays.csv`](https://docs.databricks.com/).
- Contiene información sobre:
  - Fecha del vuelo  
  - Retraso en minutos  
  - Aeropuerto de origen y destino  
  - Distancia del vuelo  

---

## 🚀 Pipeline ETL

1. **Extract**  
   - Lectura del dataset desde Databricks en formato CSV usando PySpark.

2. **Transform**  
   - Limpieza de valores nulos.  
   - Filtrado de vuelos inválidos.  
   - Categorización de retrasos:
     - `On time`
     - `Slight delay`
     - `Moderate delay`
     - `Severe delay`

3. **Load**  
   - Almacenamiento de resultados en formato **Parquet** en Databricks FileStore.  

---

## 📊 Resultados y Visualizaciones

### Distribución de vuelos por mes

![Distribución de vuelos](vuelos_por_mes.png)

### Top 10 aeropuertos con mayor retraso promedio
```python
df_clean.groupBy("origin").avg("delay").orderBy("avg(delay)", ascending=False).show(10)
