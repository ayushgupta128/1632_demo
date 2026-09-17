{{
  config({    
    "materialized": "ephemeral",
    "database": "EDW_EAST_DEV",
    "schema": "DIMENSIONAL"
  })
}}

WITH aircraft_fleet_snowflake AS (

  {#Loads airline data from the AIRLINES_AIRLINES table for further processing.#}
  SELECT * 
  
  FROM {{ source('AIRLINES_AIRLINES', 'a1h') }}

),

Reformat_1 AS (

  SELECT * 
  
  FROM aircraft_fleet_snowflake AS in0

)

SELECT *

FROM Reformat_1
