# Datasets fuente

Coloca estos archivos en la capa raw del storage configurado:

- `Movies.csv`
- `FilmDetails.csv`
- `MoreInfo.csv`
- `PosterPath.csv`

## Ruta sugerida
```text
abfss://raw@<storage-account>.dfs.core.windows.net/final_project/
```

## Nota
Aunque esta carpeta existe como referencia documental, el ETL debe leer desde **Azure Storage** mediante **Managed Identity**, no desde DBFS ni desde Volumes.
