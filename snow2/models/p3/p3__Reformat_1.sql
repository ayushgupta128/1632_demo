{{
  config({    
    "materialized": "ephemeral",
    "database": "EDW_EAST_DEV",
    "schema": "DIMENSIONAL"
  })
}}

WITH airport_directory_snowflake AS (

  {#Loads airline data from the AIRLINES_AIRLINES table for further processing in the pipeline.#}
  SELECT * 
  
  FROM {{ source('AIRLINES_AIRLINES', 'snow_alias') }}

),

Reformat_1 AS (

  SELECT * 
  
  FROM airport_directory_snowflake AS in0

)

SELECT *

FROM Reformat_1
