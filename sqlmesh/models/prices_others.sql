MODEL (
    name retail_pricing.prices_others,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key id
    ),
    dialect postgres
);

SELECT
    CAST(raw_data->>'id' AS BIGINT)                                    AS id,
    raw_data->>'product_code'                                          AS product_code,
    raw_data->>'product_name'                                          AS product_name,
    CAST(raw_data->>'price' AS NUMERIC(10,2))                          AS price,
    CAST(raw_data->>'price_is_discounted' AS BOOLEAN)                  AS price_is_discounted,
    CAST(raw_data->>'price_without_discount' AS NUMERIC(10,2))         AS price_without_discount,
    raw_data->>'discount_type'                                         AS discount_type,
    raw_data->>'price_per'                                             AS price_per,
    raw_data->>'currency'                                              AS currency,
    TO_TIMESTAMP(CAST(raw_data->>'date' AS BIGINT))::DATE              AS date,
    TO_TIMESTAMP(CAST(raw_data->>'updated' AS BIGINT) / 1000.0)        AS updated,
    CAST(raw_data->>'location_id' AS BIGINT)                           AS location_id,
    SPLIT_PART(raw_data->>'location_osm_display_name', ',', 1)         AS enseigne,
    raw_data->>'location_osm_display_name'                             AS store_name,
    raw_data->>'location_osm_address_city'                             AS city,
    raw_data->>'location_osm_address_postcode'                         AS postcode,
    LEFT(raw_data->>'location_osm_address_postcode', 2)                AS departement,
    raw_data->>'location_osm_address_country'                          AS country,
    CAST(raw_data->>'location_osm_lat' AS NUMERIC(10,6))               AS lat,
    CAST(raw_data->>'location_osm_lon' AS NUMERIC(10,6))               AS lon,
    NOW()                                                              AS inserted_at
FROM public.prices_raw
WHERE LOWER(raw_data->>'location_osm_display_name') NOT LIKE '%leclerc%'
AND   raw_data->>'price' IS NOT NULL
AND   raw_data->>'product_code' IS NOT NULL