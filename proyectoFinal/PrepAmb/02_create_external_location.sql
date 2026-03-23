
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_raw`
URL 'abfss://raw@asaproyecto.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación raw en el contenedor independiente raw';

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_bronze`
URL 'abfss://bronze@asaproyecto.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación bronze en el contenedor independiente bronze';

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_silver`
URL 'abfss://silver@asaproyecto.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación silver en el contenedor independiente silver';

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_gold`
URL 'abfss://gold@asaproyecto.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `credential`)
COMMENT 'Ubicación gold en el contenedor independiente gold';

GRANT READ FILES ON EXTERNAL LOCATION `exlt_raw` TO `datareaders`;
GRANT READ FILES ON EXTERNAL LOCATION `exlt_raw` TO `dataengineers`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt_bronze` TO `dataengineers`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt_silver` TO `dataengineers`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt_gold` TO `dataengineers`;
