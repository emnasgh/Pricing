MODEL (
    name retail_pricing.products_ref,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key code
    ),
    dialect postgres
);

SELECT DISTINCT ON (raw_data->'product'->>'code')
    raw_data->'product'->>'code'                               AS code,
    raw_data->'product'->>'product_name'                       AS product_name,
    raw_data->'product'->>'brands'                             AS brands,
    raw_data->'product'->>'nutriscore_grade'                   AS nutriscore_grade,
    raw_data->'product'->>'ecoscore_grade'                     AS ecoscore_grade,
    CAST(raw_data->'product'->>'nova_group' AS INTEGER)        AS nova_group,
    raw_data->'product'->>'source'                             AS source,
    NOW()                                                      AS inserted_at,
    NOW()                                                      AS updated_at
FROM retail_pricing.prices_raw
WHERE raw_data->'product'->>'code' IS NOT NULL
ORDER BY raw_data->'product'->>'code', inserted_at DESC