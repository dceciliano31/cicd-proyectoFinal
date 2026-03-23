-- Preparación lógica del entorno para el proyecto final.

CREATE CATALOG IF NOT EXISTS proyectoFinal
COMMENT 'Catálogo del proyecto final de ingeniería de datos con datasets de películas';

USE CATALOG proyectoFinal;

CREATE SCHEMA IF NOT EXISTS raw
COMMENT 'Zona lógica raw usada para documentación del origen';

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Zona bronze: ingestión casi cruda con trazabilidad técnica';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Zona silver: limpieza, conformado y reglas de negocio';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Zona gold: modelo estrella y vistas para Power BI';
