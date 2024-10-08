select county, state, total_female_population, total_male_population, total_population
from {{ ref('raw_demographic') }}
