{{ config(
    materialized='table',
    post_hook="
        COPY INTO {{ this }}
        FROM (
            SELECT 
                $1 AS video_id,
                $2 AS trending_date_str,
                $3 AS title,
                $4 AS channel_title,
                $5 AS category_id,
                $6 AS publish_time,
                $7 AS tags,
                $8 AS views,
                $9 AS likes,
                $10 AS dislikes,
                $11 AS comment_count,
                $12 AS thumbnail_link,
                $13 AS comments_disabled,
                $14 AS ratings_disabled,
                $15 AS video_error_or_removed,
                $16 AS description,
                METADATA$FILENAME as file_name,
                CURRENT_TIMESTAMP as etl_inserted_timestamp
            FROM @sf_videos_stg
        )
    "
) }}

SELECT
    CAST(NULL AS VARCHAR) AS video_id,
    CAST(NULL AS VARCHAR) AS trending_date_str,
    CAST(NULL AS VARCHAR) AS title,
    CAST(NULL AS VARCHAR) AS channel_title,
    CAST(NULL AS INT) AS category_id,
    CAST(NULL AS TIMESTAMP_NTZ) AS publish_time,
    CAST(NULL AS VARCHAR) AS tags,
    CAST(NULL AS BIGINT) AS views,
    CAST(NULL AS BIGINT) AS likes,
    CAST(NULL AS BIGINT) AS dislikes,
    CAST(NULL AS BIGINT) AS comment_count,
    CAST(NULL AS VARCHAR) AS thumbnail_link,
    CAST(NULL AS BOOLEAN) AS comments_disabled,
    CAST(NULL AS BOOLEAN) AS ratings_disabled,
    CAST(NULL AS BOOLEAN) AS video_error_or_removed,
    CAST(NULL AS VARCHAR) AS description,
    CAST(NULL AS VARCHAR) AS file_name,
    CAST(NULL AS TIMESTAMP_NTZ) AS etl_inserted_timestamp
WHERE 1=0
