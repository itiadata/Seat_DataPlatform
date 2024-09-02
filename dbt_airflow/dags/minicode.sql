
-- @existeparquet
select count (name_file)
from DEV_STAGING_DB.{schema}.CONTROL_PARQUETS
where name_file =  '{name}' ;

-- @firstload
select count(distinct name_file )
from DEV_STAGING_DB.{schema}.CONTROL_PARQUETS;

--@existstage
SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.STAGES
        WHERE STAGE_SCHEMA = '{schema}'
        AND STAGE_NAME = '{stage_name}';

--@createstage
CREATE STAGE IF NOT EXISTS  {stage_name};

--@addfilestage
PUT file://{name} @{stage_name};

--@createtable
CREATE TABLE IF NOT EXISTS  DEV_STAGING_DB.{schema}.{nametable}
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@{stage_name}/{name_parquet}' ,
          FILE_FORMAT=>'DEV_STAGING_DB.{schema}.my_parquet_format'
        )
      ));
   
--@createformat
CREATE FILE FORMAT IF NOT EXISTS  DEV_STAGING_DB.{schema}.my_parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY';


--@copytotable
COPY INTO DEV_STAGING_DB.{schema}.{nametable}
FROM @{stage_name}/{name_parquet}
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
ON_ERROR = 'CONTINUE';



--@removestage
REMOVE @{stage_name}/{name};

--@inserdata
INSERT INTO DEV_STAGING_DB.{schema}.CONTROL_PARQUETS (table_name, fecha, name_file)
VALUES ('{table_name}', '{fecha}', '{name_file}');

--@createcontrol
CREATE TABLE IF NOT EXISTS DEV_STAGING_DB.{schema}.CONTROL_PARQUETS  (
    table_name string,
    fecha datetime,
    name_file string
);

--@createschema
CREATE SCHEMA IF NOT EXISTS DEV_STAGING_DB.{schema};

--@defaultdatabase
USE DATABASE DEV_STAGING_DB;

