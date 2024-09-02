{{
    config(
        materialized='incremental',
        unique_key=['state','county'])
}}


select
state, county, 
        sum(total_female_population) as total_female_population_glob ,
        sum(total_male_population) as total_male_population_glob , 
        sum(total_population) as total_population_glob,
        (total_male_population_glob :: float / total_population_glob ::float )*100 as perc_men ,
        (total_female_population_glob :: float / total_population_glob ::float )*100 as perc_wom
from {{ ref('taux_demographic') }}
group by 1,2