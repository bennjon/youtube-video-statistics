{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
    )
}}

WITH base AS (
    SELECT DISTINCT
        i.value:id::INT as id,
        i.value:snippet.title::VARCHAR as title,
        i.value:snippet.assignable::BOOLEAN as assignable,
        etl_inserted_timestamp
    FROM {{ ref('category') }}
    ,LATERAL FLATTEN(INPUT => json_data:items) i

{% if is_incremental() %}

    -- Only bring in rows newer than what's in the target
    where etl_inserted_timestamp >= (select coalesce(max(etl_inserted_timestamp),'1900-01-01') from {{ this }} )

{% endif %}
)

SELECT 
    id,
    title,
    assignable,
    etl_inserted_timestamp
FROM base
