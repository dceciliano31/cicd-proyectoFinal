# 🎬 Pipeline de Ingeniería de Datos – Análisis de Películas

Arquitectura Medallion en Azure Databricks | PySpark | Delta Lake | CI/CD con GitHub Actions

---

## 🎯 Descripción

Este proyecto implementa un pipeline de ingeniería de datos end-to-end para el análisis de información de películas, utilizando Azure Databricks y siguiendo la arquitectura Medallion (Bronze, Silver, Gold).

El flujo automatiza la ingestión, transformación y modelado de datos desde archivos CSV hasta un modelo estrella optimizado para consumo en Power BI, incorporando prácticas de CI/CD para despliegue entre ambientes.

---

## 🚀 Objetivos del Proyecto

- Construir un pipeline ETL escalable en Databricks
- Implementar arquitectura Medallion
- Diseñar un modelo estrella para analítica
- Automatizar despliegue mediante GitHub Actions
- Habilitar consumo en Power BI

---

## 🏗️ Arquitectura

### Flujo de Datos
📄 CSV (Raw)
↓
🥉 Bronze (Ingesta)
↓
🥈 Silver (Transformación)
↓
🥇 Gold (Modelo estrella)
↓
📊 Power BI


---

## 📦 Capas del Pipeline

### 🥉 Bronze Layer
**Propósito:** Ingesta de datos crudos

- Lectura directa de archivos CSV
- Sin transformaciones complejas
- Inclusión de metadata técnica
- Persistencia en Delta Lake

---

### 🥈 Silver Layer
**Propósito:** Limpieza y estandarización

- Normalización de datos
- Manejo de nulos y tipos
- Generación de datasets conformados
- Preparación para modelo dimensional

---

### 🥇 Gold Layer
**Propósito:** Modelo analítico

- Implementación de modelo estrella
- Tablas:
  - `fact_movie_metrics`
  - `dim_movie`
  - `dim_director`
  - `dim_language`
  - `dim_date`
  - `dim_genre`
  - `bridge_movie_genre`
- Creación de vistas para Power BI:
  - `vw_powerbi_movie_finance`
  - `vw_powerbi_movie_genre`

---

## 📊 Visualización

El modelo Gold se consume en Power BI para generar dashboards analíticos:

- KPIs financieros
- análisis por género
- tendencias temporales
- ranking de películas

---

## 📁 Estructura del Proyecto
ProyectoFinal/
│
├── .github/
│ └── workflows/
│ └── deploy-production.yml
│
├── PrepAmb/
│ ├── 01_create_catalog_and_schemas.py
│ ├── 02_create_external_location.py
│ └── 03_create_gold_views.py
│
├── proceso/
│ ├── 00_config.py
│ ├── 01_raw_to_bronze.py
│ ├── 02_bronze_to_silver.py
│ ├── 03_silver_to_gold_star_schema.py
│ └── 04_data_quality_checks.py
│
├── seguridad/
│ └── 01_grants.sql
│
├── reversion/
│ └── 01_drop_objects.sql
│
├── dashboard/
│ └── proyectoFinal_movies.pbix
│
└── README.md


---

## 🛠️ Tecnologías Utilizadas

| Tecnología | Uso |
|----------|-----|
| Azure Databricks | Procesamiento distribuido |
| PySpark | Transformaciones de datos |
| Delta Lake | Almacenamiento con ACID |
| ADLS Gen2 | Data Lake |
| GitHub Actions | CI/CD |
| Power BI | Visualización |

---

## ⚙️ Requisitos

- Workspace de Azure Databricks
- Cluster configurado (`cl_proyectoFinal`)
- Azure Data Lake Storage
- Cuenta GitHub
- Power BI Desktop (opcional)

---

## 🔐 Configuración

### GitHub Secrets
DATABRICKS_ORIGIN_HOST
DATABRICKS_ORIGIN_TOKEN

DATABRICKS_DEST_HOST
DATABRICKS_DEST_TOKEN


---

## 🔄 CI/CD

El pipeline implementa un flujo multiambiente:

### 🔹 DEV (Origen)
- Desarrollo y pruebas
- Validación de notebooks

### 🔹 PROD (Destino)
- Despliegue automatizado
- Ejecución del pipeline completo

---

## 🔁 Proceso de Despliegue

Al hacer push a `main`:

1. Validación de conexión a entornos
2. Despliegue de notebooks a Databricks PROD
3. Creación del workflow:
   `WF_proyectoFinal_prod`
4. Ejecución automática del pipeline:
   - PrepAmb
   - Bronze
   - Silver
   - Gold
   - Data Quality

---

## ▶️ Ejecución Manual

En Databricks:

Ejecutar en orden:

---

## 🔄 CI/CD

El pipeline implementa un flujo multiambiente:

### 🔹 DEV (Origen)
- Desarrollo y pruebas
- Validación de notebooks

### 🔹 PROD (Destino)
- Despliegue automatizado
- Ejecución del pipeline completo

---

## 🔁 Proceso de Despliegue

Al hacer push a `main`:

1. Validación de conexión a entornos
2. Despliegue de notebooks a Databricks PROD
3. Creación del workflow:
   `WF_proyectoFinal_prod`
4. Ejecución automática del pipeline:
   - PrepAmb
   - Bronze
   - Silver
   - Gold
   - Data Quality

---

## ▶️ Ejecución Manual

En Databricks:

Ejecutar en orden:
PrepAmb/01_create_catalog_and_schemas
PrepAmb/02_create_external_location
proceso/01_raw_to_bronze
proceso/02_bronze_to_silver
proceso/03_silver_to_gold_star_schema
PrepAmb/03_create_gold_views
proceso/04_data_quality_checks


---

## 📈 Monitoreo

### En Databricks
- Workflows → `WF_proyectoFinal_prod`
- Logs por tarea

### En GitHub
- Actions → historial de ejecuciones
- Logs detallados por step

---

## 🧪 Validación

Consultas de verificación:

```sql
SHOW TABLES IN proyectoFinal.gold;

SELECT COUNT(*) FROM proyectoFinal.gold.fact_movie_metrics;
SELECT COUNT(*) FROM proyectoFinal.gold.dim_movie;
SELECT COUNT(*) FROM proyectoFinal.gold.vw_powerbi_movie_finance;

👤 Autor

Denison Ceciliano
Ingeniería de Datos | Azure | Databricks | CI/CD
