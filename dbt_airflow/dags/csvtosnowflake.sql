--@createtable
CREATE TABLE IF NOT EXISTS  {schema}.{name_table}
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@{stage_name}/{name_table}' ,
          FILE_FORMAT=>'{schema}.CSV_FILE_FORMAT'
        )
      ));

--@userwarehouse
USE WAREHOUSE COMPUTE_DEV;



--@createstage
CREATE STAGE IF NOT EXISTS {stage_name};

--@databasedefintion
USE DATABASE SANDBOX;


--@addfilestage
PUT file://{path_route} @{stage_name};

--@fileformat_csv

CREATE OR REPLACE FILE FORMAT {schema}.CSV_FILE_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = '{delimiter}'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    PARSE_HEADER = TRUE
    TRIM_SPACE = TRUE
    NULL_IF = ('NULL', '')
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'AUTO'
    TIME_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO';




--@copyinto
COPY INTO {schema}.{name_table}
FROM @{stage_name}/{name_table}
FILE_FORMAT = (FORMAT_NAME = '{schema}.CSV_FILE_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
ON_ERROR = 'CONTINUE';
