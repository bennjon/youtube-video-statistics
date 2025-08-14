SELECT
    v.category_id
FROM {{ ref('videos_r') }} v
LEFT JOIN {{ ref('category_r') }} c
    ON v.category_id = c.id
WHERE c.id IS NULL