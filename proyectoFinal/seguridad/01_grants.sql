USE CATALOG proyectoFinal;

GRANT USE CATALOG ON CATALOG proyectoFinal TO `datareaders`;
GRANT USE CATALOG ON CATALOG proyectoFinal TO `dataengineers`;

GRANT USE SCHEMA ON SCHEMA proyectoFinal.bronze TO `datareaders`;
GRANT USE SCHEMA ON SCHEMA proyectoFinal.silver TO `datareaders`;
GRANT USE SCHEMA ON SCHEMA proyectoFinal.gold TO `datareaders`;

GRANT USE SCHEMA ON SCHEMA proyectoFinal.bronze TO `dataengineers`;
GRANT USE SCHEMA ON SCHEMA proyectoFinal.silver TO `dataengineers`;
GRANT USE SCHEMA ON SCHEMA proyectoFinal.gold TO `dataengineers`;

GRANT SELECT ON SCHEMA proyectoFinal.bronze TO `datareaders`;
GRANT SELECT ON SCHEMA proyectoFinal.silver TO `datareaders`;
GRANT SELECT ON SCHEMA proyectoFinal.gold TO `datareaders`;

GRANT SELECT, MODIFY ON SCHEMA proyectoFinal.bronze TO `dataengineers`;
GRANT SELECT, MODIFY ON SCHEMA proyectoFinal.silver TO `dataengineers`;
GRANT SELECT, MODIFY ON SCHEMA proyectoFinal.gold TO `dataengineers`;
