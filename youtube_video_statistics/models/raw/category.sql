{{ config(
    materialized='table',
    post_hook="
        COPY INTO {{ this }}
        FROM (
            SELECT 
                $1 AS json_data,
                CURRENT_TIMESTAMP as etl_inserted_timestamp
            FROM @raw.sf_category_stg
        )
    "
) }}

SELECT
    CAST(NULL AS VARIANT) AS json_data,
    CAST(NULL AS TIMESTAMP_NTZ) AS etl_inserted_timestamp
WHERE 1=0
