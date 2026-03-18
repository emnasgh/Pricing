MODEL (
    name retail_pricing.products_ref,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key code
    ),
    dialect postgres
);

WITH ranked AS (
    SELECT
        raw_data->>'product_code'                                      AS code,
        raw_data->>'product_name'                                      AS product_name,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data->>'product_code'
            ORDER BY api_updated DESC NULLS LAST
        ) AS rn
    FROM public.prices_raw
    WHERE raw_data->>'product_code' IS NOT NULL
)
SELECT
    code,
    product_name,
    NOW() AS inserted_at,
    NOW() AS updated_at
FROM ranked
WHERE rn = 1