{{ config(
  materialized='table'
) }}

-- Your transformation logic here, for example:
WITH demographics AS (
  SELECT
    *
  FROM {{ source('COVID19_EPIDEMIOLOGICAL_DATA', 'DEMOGRAPHICS') }}


)

SELECT *
FROM demographics
