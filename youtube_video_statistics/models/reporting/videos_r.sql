{{
    config(
        materialized='incremental',
        unique_key=['video_id','trending_date', 'file_names'],
        incremental_strategy='merge'
    )
}}
WITH base as (
SELECT
    CASE WHEN video_id = '#NAME?'
        THEN REGEXP_SUBSTR(
        thumbnail_link,
        '/vi/([^/]+)/default\\.jpg',
        1, 1, 'e', 1)
        ELSE video_id
    END AS video_id,
    TO_DATE(trending_date_str, 'YY.DD.MM') AS trending_date,
    title,
    channel_title,
    category_id,
    publish_time,
    tags,
    views,
    likes,
    dislikes,
    comment_count,
    thumbnail_link,
    comments_disabled,
    ratings_disabled,
    video_error_or_removed,
    description,
    file_name,
    etl_inserted_timestamp
FROM {{ ref('videos') }}

{% if is_incremental() %}
  -- Only bring in rows newer than what's in the target
where etl_inserted_timestamp >= (select coalesce(max(etl_inserted_timestamp),'1900-01-01') from {{ this }} )

{% endif %}
), agg AS (

SELECT
    video_id,
    trending_date,
    title,
    channel_title,
    category_id,
    publish_time,
    ARRAY_AGG(tags) AS tags,
    --Assume that if there are duplicates across files then the metrics are the same so we will take max
    MAX(views) AS views,
    MAX(likes) AS likes,
    MAX(dislikes) AS dislikes,
    MAX(comment_count) AS comment_count,
    thumbnail_link,
    comments_disabled,
    ratings_disabled,
    video_error_or_removed,
    description,
    ARRAY_AGG(file_name) AS file_names,
    etl_inserted_timestamp
FROM base
GROUP BY video_id, trending_date, title, channel_title, category_id, publish_time, thumbnail_link, comments_disabled, ratings_disabled, video_error_or_removed, description, etl_inserted_timestamp

)

SELECT 
    video_id,
    trending_date,
    title,
    channel_title,
    category_id,
    publish_time,
    tags,
    views,
    likes,
    dislikes,
    comment_count,
    thumbnail_link,
    comments_disabled,
    ratings_disabled,
    video_error_or_removed,
    description,
    file_names,
    etl_inserted_timestamp
FROM agg