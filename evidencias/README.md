# Guía de evidencias

Capturas recomendadas para el entregable:

1. **Azure Storage**
   - Contenedor `raw`
   - Contenedores `raw`, `bronze`, `silver` y `gold`
   - Los 4 archivos CSV visibles
   - Guardar como `01_storage_raw.png`

2. **Unity Catalog**
   - Catálogo `proyectoFinal`
   - Schemas `bronze`, `silver`, `gold`
   - Guardar como `02_unity_catalog.png`

3. **External Locations**
   - `exlt_raw`, `exlt_bronze`, `exlt_silver`, `exlt_gold`
   - Guardar como `03_external_locations.png`

4. **Ejecución bronze**
   - Resultado del notebook `01_raw_to_bronze.py`
   - Conteos cargados por tabla
   - Guardar como `04_bronze_load.png`

5. **Ejecución silver**
   - Resultado del notebook `02_bronze_to_silver.py`
   - Mostrar `movie_conformed`
   - Guardar como `05_silver_conformed.png`

6. **Ejecución gold**
   - Resultado del notebook `03_silver_to_gold_star_schema.py`
   - Tablas `dim_movie` y `fact_movie_metrics`
   - Guardar como `06_gold_star_schema.png`

7. **Data quality**
   - Resultado del notebook `04_data_quality_checks.py`
   - Guardar como `07_quality_checks.png`

8. **Views para Power BI**
   - Vista `vw_powerbi_movie_finance`
   - Guardar como `08_gold_views.png`

9. **Permisos**
   - Evidencia de grants sobre `datareaders` y `dataengineers`
   - Guardar como `09_grants.png`

10. **GitHub Actions**
    - Workflow `databricks-ci-cd` ejecutado
    - Guardar como `10_github_actions.png`

11. **Power BI**
    - Modelo estrella importado
    - Dashboard final
    - Guardar como `11_powerbi_model.png` y `12_powerbi_dashboard.png`
