--@createtable
CREATE TABLE IF NOT EXISTS  rvs_table.{name_table}
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@RVS.RVS_TABLE.RVS_FILE_CSV/{name_csv}' ,
          FILE_FORMAT=>'RVS.RVS_TABLE.CSV_FILE_FORMAT'
        )
      ));



--@createstage
CREATE STAGE IF NOT EXISTS rvs_table.RVS_FILE_CSV;

--@databasedefintion
USE DATABASE RVS;


--@addfilestage
PUT file://{path_route} @{stage_name};

--@fileformat_csv

CREATE OR REPLACE FILE FORMAT  rvs_table.CSV_FILE_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = '{delimiter}'
    PARSE_HEADER = TRUE
    TRIM_SPACE = TRUE
    NULL_IF = ('NULL')
    COMPRESSION = 'GZIP';


--@fileformat_parquet
CREATE FILE FORMAT IF NOT EXISTS  rvs_table.PARQUET_FILE_FORMAT
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY';

--@copyinto



COPY INTO rvs_table.{name_table}
FROM @{stage_name}/{name_csv}
FILE_FORMAT = (FORMAT_NAME = 'RVS.RVS_TABLE.CSV_FILE_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
ON_ERROR = 'CONTINUE';



